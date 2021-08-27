/*
 * Copyright © 2021 ProStore
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
package io.arenadata.dtm.query.execution.core.ddl.service.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateView;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.view.ViewDisalowedOrDirectiveException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.ddl.utils.SqlPreparer;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.arenadata.dtm.query.execution.core.ddl.utils.ValidationUtils.checkTimestampFormat;

@Slf4j
@Component
public class CreateViewExecutor extends QueryResultDdlExecutor {
    private static final String VIEW_AND_TABLE_PATTERN = "(?i).*(JOIN(|\\[\\d+\\])|SELECT)\\.(|AS\\.)(SNAPSHOT|IDENTIFIER)$";
    private static final String ALL_COLUMNS = "*";
    protected final SqlDialect sqlDialect;
    protected final EntityDao entityDao;
    protected final CacheService<EntityKey, Entity> entityCacheService;
    protected final LogicalSchemaProvider logicalSchemaProvider;
    protected final QueryParserService parserService;
    private final ColumnMetadataService columnMetadataService;
    private final DtmRelToSqlConverter relToSqlConverter;

    @Autowired
    public CreateViewExecutor(@Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                              MetadataExecutor<DdlRequestContext> metadataExecutor,
                              LogicalSchemaProvider logicalSchemaProvider,
                              ColumnMetadataService columnMetadataService,
                              ServiceDbFacade serviceDbFacade,
                              @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                              @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService,
                              @Qualifier("coreRelToSqlConverter") DtmRelToSqlConverter relToSqlConverter) {
        super(metadataExecutor, serviceDbFacade);
        this.entityCacheService = entityCacheService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.sqlDialect = sqlDialect;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.parserService = parserService;
        this.relToSqlConverter = relToSqlConverter;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return checkViewQuery(context)
                .compose(v -> parseSelect(((SqlCreateView) context.getSqlNode()).getQuery(), context.getDatamartName()))
                .map(parserResponse -> {
                    checkTimestampFormat(parserResponse.getSqlNode());
                    return parserResponse;
                })
                .compose(response -> getCreateViewContext(context, response))
                .compose(this::createOrReplaceEntity);
    }

    protected Future<Void> checkViewQuery(DdlRequestContext context) {
        val datamartName = context.getDatamartName();
        val sqlNode = context.getSqlNode();
        return checkSnapshotNotExist(sqlNode)
                .compose(v -> checkEntitiesType(sqlNode, datamartName));
    }

    protected Future<QueryParserResponse> parseSelect(SqlNode viewQuery, String datamart) {
        return logicalSchemaProvider.getSchemaFromQuery(viewQuery, datamart)
                .compose(datamarts -> parserService.parse(new QueryParserRequest(viewQuery, datamarts)));
    }

    protected Future<CreateViewContext> getCreateViewContext(DdlRequestContext context, QueryParserResponse parserResponse) {
        return Future.future(p -> {
            val selectSqlNode = getParsedSelect(context.getSqlNode(), parserResponse);
            val isCreateOrReplace = SqlPreparer.isCreateOrReplace(context.getRequest().getQueryRequest().getSql());
            replaceSqlSelectQuery(context, isCreateOrReplace, selectSqlNode);
            getEntityFuture(context, selectSqlNode, parserResponse.getSchema())
                    .map(entity -> {
                        context.setEntity(entity);
                        return CreateViewContext.builder()
                                .createOrReplace(isCreateOrReplace)
                                .viewEntity(entity)
                                .build();
                    })
                    .onComplete(p);
        });
    }

    private SqlNode getParsedSelect(SqlNode originalSqlNode, QueryParserResponse response) {
        if (isAllColumnSelect(originalSqlNode)) {
            return response.getSqlNode();
        } else {
            return relToSqlConverter.convert(response.getRelNode().project());
        }
    }

    private boolean isAllColumnSelect(SqlNode sqlNode) {
        return sqlNode.toSqlString(sqlDialect).getSql().contains(ALL_COLUMNS);
    }

    @SneakyThrows
    protected void replaceSqlSelectQuery(DdlRequestContext context, boolean replace, SqlNode newSelectNode) {
        val sql = (SqlCreateView) context.getSqlNode();
        val newSql = new SqlCreateView(sql.getParserPosition(), replace, sql.getName(), sql.getColumnList(), newSelectNode);
        context.setSqlNode(newSql);
    }

    private Future<QueryResult> createOrReplaceEntity(CreateViewContext ctx) {
        val viewEntity = ctx.getViewEntity();
        entityCacheService.remove(new EntityKey(viewEntity.getSchema(), viewEntity.getName()));
        return Future.future(p -> entityDao.createEntity(viewEntity)
                .onSuccess(ar -> p.complete(QueryResult.emptyResult()))
                .onFailure(error -> {
                    if (checkCreateOrReplace(ctx.isCreateOrReplace(), error)) {
                        entityDao.getEntity(viewEntity.getSchema(), viewEntity.getName())
                                .compose(this::checkEntityType)
                                .compose(r -> entityDao.updateEntity(viewEntity))
                                .map(v -> QueryResult.emptyResult())
                                .onSuccess(p::complete)
                                .onFailure(p::fail);
                    } else {
                        p.fail(error);
                    }
                })
        );
    }

    private Future<Void> checkSnapshotNotExist(SqlNode sqlNode) {
        return Future.future(p -> {
            List<SqlTreeNode> bySnapshot = new SqlSelectTree(sqlNode)
                    .findNodesByPath(SqlSelectTree.SELECT_AS_SNAPSHOT);
            if (bySnapshot.isEmpty()) {
                p.complete();
            } else {
                p.fail(new ViewDisalowedOrDirectiveException(sqlNode.toSqlString(sqlDialect).getSql()));
            }
        });
    }

    private Future<Void> checkEntitiesType(SqlNode sqlNode, String contextDatamartName) {
        return Future.future(promise -> {
            final List<SqlTreeNode> nodes = new SqlSelectTree(sqlNode)
                    .findNodesByPathRegex(VIEW_AND_TABLE_PATTERN);
            final List<Future> entityFutures = getEntitiesFutures(contextDatamartName, sqlNode, nodes);
            CompositeFuture.join(entityFutures)
                    .onSuccess(result -> {
                        final List<Entity> entities = result.list();
                        if (entities.stream().anyMatch(entity -> entity.getEntityType() != EntityType.TABLE)) {
                            promise.fail(new ViewDisalowedOrDirectiveException(
                                    sqlNode.toSqlString(sqlDialect).getSql()));
                        }
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    @NotNull
    private List<Future> getEntitiesFutures(String contextDatamartName, SqlNode sqlNode, List<SqlTreeNode> nodes) {
        final List<Future> entityFutures = new ArrayList<>();
        nodes.forEach(node -> {
            String datamartName = contextDatamartName;
            String tableName;
            final Optional<String> schema = node.tryGetSchemaName();
            final Optional<String> table = node.tryGetTableName();
            if (schema.isPresent()) {
                datamartName = schema.get();
            }
            if (table.isPresent()) {
                tableName = table.get();
            } else {
                throw new DtmException(String.format("Can't extract table name from query %s",
                        sqlNode.toSqlString(sqlDialect).toString()));
            }
            entityCacheService.remove(new EntityKey(datamartName, tableName));
            entityFutures.add(entityDao.getEntity(datamartName, tableName));
        });
        return entityFutures;
    }

    private Future<Entity> getEntityFuture(DdlRequestContext ctx, SqlNode viewQuery, List<Datamart> datamarts) {
        return columnMetadataService.getColumnMetadata(new QueryParserRequest(viewQuery, datamarts))
                .map(columnMetadata -> toViewEntity(ctx, viewQuery, columnMetadata));
    }

    @SneakyThrows
    private Entity toViewEntity(DdlRequestContext ctx, SqlNode viewQuery, List<ColumnMetadata> columnMetadata) {
        val tree = new SqlSelectTree(ctx.getSqlNode());
        val viewNameNode = SqlPreparer.getViewNameNode(tree);
        val schemaName = viewNameNode.tryGetSchemaName()
                .orElseThrow(() -> new DtmException("Unable to get schema of view"));
        val viewName = viewNameNode.tryGetTableName()
                .orElseThrow(() -> new DtmException("Unable to get name of view"));
        val viewQueryString = viewQuery.toSqlString(sqlDialect)
                .getSql()
                .replace("\n", " ").replace("\r", "");
        ctx.setDatamartName(schemaName);
        return Entity.builder()
                .name(viewName)
                .schema(schemaName)
                .entityType(EntityType.VIEW)
                .viewQuery(viewQueryString)
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
                .size(cm.getSize())
                .ordinalPosition(position)
                .build();
    }

    @SneakyThrows
    private boolean checkCreateOrReplace(boolean isCreateOrReplace, Throwable error) {
        // if there is an exception <entity already exists> and <orReplace> is true
        // then continue
        return error instanceof EntityAlreadyExistsException && isCreateOrReplace;
    }

    protected Future<Void> checkEntityType(Entity entity) {
        if (EntityType.VIEW == entity.getEntityType()) {
            return Future.succeededFuture();
        }
        return Future.failedFuture(new EntityNotExistsException(entity.getName()));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_VIEW;
    }

    @Data
    @Builder
    protected static final class CreateViewContext {
        private final boolean createOrReplace;
        private final Entity viewEntity;
    }
}
