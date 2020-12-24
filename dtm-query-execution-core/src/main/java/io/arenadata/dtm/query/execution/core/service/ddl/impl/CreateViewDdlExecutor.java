/*
 * Copyright © 2020 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.exception.entity.EntityAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.dao.exception.entity.ViewNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.utils.SqlPreparer;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Component
public class CreateViewDdlExecutor extends QueryResultDdlExecutor {
    private static final String VIEW_QUERY_PATH = "CREATE_VIEW.SELECT";
    private static final String VIEW_AND_TABLE_PATTERN = "(?i).*(JOIN|SELECT)\\.(|AS\\.)(SNAPSHOT|IDENTIFIER)$";
    protected final SqlDialect sqlDialect;
    protected final EntityDao entityDao;
    protected final EntityCacheService entityCacheService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final ColumnMetadataService columnMetadataService;

    @Autowired
    public CreateViewDdlExecutor(@Qualifier("entityCacheService") EntityCacheService entityCacheService,
                                 MetadataExecutor<DdlRequestContext> metadataExecutor,
                                 LogicalSchemaProvider logicalSchemaProvider,
                                 ColumnMetadataService columnMetadataService,
                                 ServiceDbFacade serviceDbFacade,
                                 @Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        super(metadataExecutor, serviceDbFacade);
        this.entityCacheService = entityCacheService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.sqlDialect = sqlDialect;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        checkViewQuery(context)
            .compose(v -> getCreateViewContext(context))
            .onFailure(error -> handler.handle(Future.failedFuture(error)))
            .onSuccess(ctx -> createOrReplaceEntity(ctx, handler));
    }

    protected Future<Void> checkViewQuery(DdlRequestContext context) {
        return checkSnapshotNotExist(context)
            .compose(v -> checkEntitiesType(context));
    }

    private Future<Void> checkSnapshotNotExist(DdlRequestContext context) {
        return Future.future(p -> {
            List<SqlTreeNode> bySnapshot = new SqlSelectTree(context.getQuery())
                .findNodesByPath(SqlSelectTree.SELECT_AS_SNAPSHOT);
            if (bySnapshot.isEmpty()) {
                p.complete();
            } else {
                p.fail("View system_time not allowed");
            }
        });
    }

    private Future<Void> checkEntitiesType(DdlRequestContext context) {
        return Future.future(promise -> {
            final List<SqlTreeNode> nodes = new SqlSelectTree(context.getQuery())
                .findNodesByPathRegex(VIEW_AND_TABLE_PATTERN);
            final List<Future> entityFutures = getEntitiesFutures(context, nodes);
            CompositeFuture.join(entityFutures)
                .onSuccess(result -> {
                    final List<Object> entities = result.list();
                    entities.forEach(e -> {
                        final Entity entity = (Entity) e;
                        if (entity.getEntityType() != EntityType.TABLE) {
                            promise.fail(new RuntimeException(
                                String.format("View query supports only %s type, but actual %s",
                                    EntityType.TABLE, entity.getEntityType())));
                        }
                    });
                    promise.complete();
                })
                .onFailure(promise::fail);
        });
    }

    @NotNull
    private List<Future> getEntitiesFutures(DdlRequestContext context, List<SqlTreeNode> nodes) {
        final List<Future> entityFutures = new ArrayList<>();
        nodes.forEach(node -> {
            String datamartName = context.getRequest().getQueryRequest().getDatamartMnemonic();
            String tableName;
            final Optional<String> schema = node.tryGetSchemaName();
            final Optional<String> table = node.tryGetTableName();
            if (schema.isPresent()) {
                datamartName = schema.get();
            }
            if (table.isPresent()) {
                tableName = table.get();
            } else {
                throw new RuntimeException(String.format("Can't extract table name from query %s",
                    context.getQuery().toSqlString(sqlDialect).toString()));
            }
            entityCacheService.remove(datamartName, tableName);
            entityFutures.add(entityDao.getEntity(datamartName, tableName));
        });
        return entityFutures;
    }

    protected Future<CreateViewContext> getCreateViewContext(DdlRequestContext context) {
        return Future.future(p -> {
            val tree = new SqlSelectTree(context.getQuery());
            val viewQuery = getViewQuery(tree);
            QueryRequest request = QueryRequest.builder()
                .datamartMnemonic(context.getDatamartName())
                .requestId(UUID.randomUUID())
                .sql(viewQuery)
                .build();
            getEntityFuture(context, request)
                .map(entity -> {
                    String sql = context.getRequest().getQueryRequest().getSql();
                    return CreateViewContext.builder()
                        .createOrReplace(SqlPreparer.isCreateOrReplace(sql))
                        .viewEntity(entity)
                        .sql(sql)
                        .build();
                })
                .onComplete(p);
        });
    }

    private Future<Entity> getEntityFuture(DdlRequestContext ctx, QueryRequest request) {
        return getLogicalSchema(request)
            .compose(datamarts -> getColumnMetadata(request, datamarts))
            .map(columnMetadata -> toViewEntity(ctx, columnMetadata));
    }

    private Future<List<Datamart>> getLogicalSchema(QueryRequest request) {
        return Future.future(p -> logicalSchemaProvider.getSchema(request, p));
    }

    private Future<List<ColumnMetadata>> getColumnMetadata(QueryRequest request, List<Datamart> datamarts) {
        return Future.future(p -> columnMetadataService.getColumnMetadata(new QueryParserRequest(request, datamarts), p));
    }

    private Entity toViewEntity(DdlRequestContext ctx, List<ColumnMetadata> columnMetadata) {
        val tree = new SqlSelectTree(ctx.getQuery());
        val viewNameNode = SqlPreparer.getViewNameNode(tree);
        val schemaName = viewNameNode.tryGetSchemaName()
            .orElseThrow(() -> new RuntimeException("Unable to get schema of view"));
        val viewName = viewNameNode.tryGetTableName()
            .orElseThrow(() -> new RuntimeException("Unable to get name of view"));
        val viewQuery = getViewQuery(tree);
        ctx.setDatamartName(schemaName);
        return Entity.builder()
            .name(viewName)
            .schema(schemaName)
            .entityType(EntityType.VIEW)
            .viewQuery(viewQuery)
            .fields(getEntityFields(columnMetadata))
            .build();
    }

    private List<EntityField> getEntityFields(List<ColumnMetadata> columnMetadata) {
        return IntStream.range(0, columnMetadata.size())
            .mapToObj(position -> toEntityField(columnMetadata.get(position), position))
            .collect(Collectors.toList());
    }

    private EntityField toEntityField(ColumnMetadata cm, int position) {
        return EntityField.builder()
            .name(cm.getName())
            .nullable(true)
            .type(cm.getType())
            .ordinalPosition(position)
            .build();
    }

    protected String getViewQuery(SqlSelectTree tree) {
        val queryByView = tree.findNodesByPath(VIEW_QUERY_PATH);
        if (queryByView.isEmpty()) {
            throw new IllegalArgumentException("Unable to get view query");
        } else {
            return queryByView.get(0).getNode().toSqlString(sqlDialect).toString();
        }
    }

    private void createOrReplaceEntity(CreateViewContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        val viewEntity = ctx.getViewEntity();
        entityCacheService.remove(viewEntity.getSchema(), viewEntity.getName());
        entityDao.createEntity(viewEntity)
            .otherwise(error -> checkCreateOrReplace(ctx, error))
            .compose(r -> entityDao.getEntity(viewEntity.getSchema(), viewEntity.getName()))
            .map(this::checkEntityType)
            .compose(r -> entityDao.updateEntity(viewEntity))
            .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
            .onFailure(error -> handler.handle(Future.failedFuture(error)));
    }

    private Void checkCreateOrReplace(CreateViewContext ctx, Throwable error) {
        if (error instanceof EntityAlreadyExistsException && ctx.isCreateOrReplace()) {
            // if there is an exception <entity already exists> and <orReplace> is true
            // then continue
            return null;
        } else {
            throw new RuntimeException(error);
        }
    }

    protected Future<Void> checkEntityType(Entity entity) {
        if (EntityType.VIEW == entity.getEntityType()) {
            return Future.succeededFuture();
        } else {
            return Future.failedFuture(new ViewNotExistsException(entity.getName()));
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_VIEW;
    }

    @Data
    @Builder
    protected final static class CreateViewContext {
        private final boolean createOrReplace;
        private final Entity viewEntity;
        private final String sql;
    }
}
