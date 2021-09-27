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
import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlDropTable;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.base.utils.InformationSchemaUtils;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DropTableExecutor extends QueryResultDdlExecutor {
    private static final String MATERIALIZED_VIEW_PREFIX = "SYS_";

    private final DataSourcePluginService dataSourcePluginService;
    private final CacheService<EntityKey, Entity> entityCacheService;
    private final EntityDao entityDao;
    private final HSQLClient hsqlClient;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public DropTableExecutor(@Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                             MetadataExecutor<DdlRequestContext> metadataExecutor,
                             ServiceDbFacade serviceDbFacade,
                             DataSourcePluginService dataSourcePluginService,
                             HSQLClient hsqlClient,
                             EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        super(metadataExecutor, serviceDbFacade);
        this.entityCacheService = entityCacheService;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.dataSourcePluginService = dataSourcePluginService;
        this.hsqlClient = hsqlClient;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            val datamartName = getSchemaName(context.getDatamartName(), sqlNodeName);
            val tableName = getTableName(sqlNodeName);
            entityCacheService.remove(new EntityKey(datamartName, tableName));

            val entity = createClassTable(datamartName, tableName);
            context.setEntity(entity);
            context.setDatamartName(datamartName);
            context.setSourceType(getSourceType(context));

            dropTable(context, containsIfExistsCheck(context.getRequest().getQueryRequest().getSql()))
                    .onSuccess(r -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    protected SourceType getSourceType(DdlRequestContext context) {
        return ((SqlDropTable) context.getSqlNode()).getDestination();
    }

    private Entity createClassTable(String schema, String tableName) {
        return new Entity(getTableNameWithSchema(schema, tableName), null);
    }

    protected Future<Void> dropTable(DdlRequestContext context, boolean ifExists) {
        return getEntity(context, ifExists)
                .compose(entity -> entity != null ?
                        checkViewsAndUpdateEntity(context, entity, ifExists) :
                        Future.succeededFuture());
    }

    private boolean containsIfExistsCheck(String sql) {
        return sql.toLowerCase().contains("if exists");
    }

    private Future<Entity> getEntity(DdlRequestContext context, boolean ifExists) {
        return Future.future(entityPromise -> {
            val datamartName = context.getDatamartName();
            val entityName = context.getEntity().getName();
            entityDao.getEntity(datamartName, entityName)
                    .onSuccess(entityPromise::complete)
                    .onFailure(error -> {
                        if (error instanceof EntityNotExistsException && ifExists) {
                            entityPromise.complete(null);
                        } else {
                            entityPromise.fail(new EntityNotExistsException(datamartName, entityName));
                        }
                    });
        });
    }

    private Future<Void> checkViewsAndUpdateEntity(DdlRequestContext context, Entity entity, boolean ifExists) {
        return checkRelatedViews(entity)
                .compose(e -> updateEntity(context, e, ifExists));
    }

    private Future<Void> updateEntity(DdlRequestContext context, Entity entity, boolean ifExists) {
        try {
            evictQueryTemplateCacheService.evictByEntityName(entity.getSchema(), entity.getName());
        } catch (Exception e) {
            return Future.failedFuture(new DtmException("Evict cache error", e));
        }

        val requestDestination = context.getSourceType();
        if (requestDestination == null) {
            val dataSourcesForDeletion = dataSourcePluginService.getSourceTypes();
            dataSourcesForDeletion.retainAll(entity.getDestination());
            context.getEntity().setDestination(dataSourcesForDeletion);
            return dropEntityFromEverywhere(context, entity.getName());
        } else {
            return dropFromDataSource(context, entity, requestDestination, ifExists);
        }
    }

    private Future<Void> dropFromDataSource(DdlRequestContext context,
                                            Entity entity,
                                            SourceType requestDestination,
                                            boolean ifExists) {
        if (!entity.getDestination().contains(requestDestination)) {
            return ifExists ? Future.succeededFuture() : Future.failedFuture(
                    new DtmException(String.format("Table [%s] doesn't exist in [%s]",
                            entity.getName(),
                            requestDestination)));
        } else {
            //find corresponding datasources in request and active plugins configuration
            val activeDataSources = dataSourcePluginService.getSourceTypes();
            if (!activeDataSources.contains(requestDestination)) {
                entity.setDestination(entity.getDestination().stream()
                        .filter(type -> !requestDestination.equals(type))
                        .collect(Collectors.toSet()));
                context.getEntity().setDestination(entity.getDestination());
                return entityDao.updateEntity(entity);
            } else {
                entity.setDestination(entity.getDestination().stream()
                        .filter(type -> !requestDestination.equals(type))
                        .collect(Collectors.toSet()));
                context.getEntity().setDestination(Collections.singleton(requestDestination));
                if (entity.getDestination().isEmpty()) {
                    return dropEntityFromEverywhere(context, entity.getName());
                } else {
                    return executeRequest(context)
                            .map(v -> {
                                context.getEntity().setDestination(entity.getDestination());
                                return v;
                            })
                            .compose(v -> entityDao.updateEntity(entity));
                }
            }
        }
    }

    private Future<Void> dropEntityFromEverywhere(DdlRequestContext context, String entityName) {
        return executeRequest(context)
                .map(v -> {
                    context.getEntity().setDestination(Collections.emptySet());
                    return v;
                })
                .compose(v -> entityDao.deleteEntity(context.getDatamartName(), entityName));
    }

    private Future<Entity> checkRelatedViews(Entity entity) {
        return Future.future(promise -> hsqlClient.getQueryResult(String.format(InformationSchemaUtils.CHECK_VIEW, entity.getSchema().toUpperCase(), entity.getName().toUpperCase()))
                .onSuccess(resultSet -> {
                    if (resultSet.getResults().isEmpty()) {
                        promise.complete(entity);
                    } else {
                        JsonArray views = resultSet.getResults().get(0);
                        List<String> viewNames = views
                                .stream()
                                .map(Object::toString)
                                .map(view -> {
                                    if (view.startsWith(MATERIALIZED_VIEW_PREFIX)) {
                                        return view.substring(MATERIALIZED_VIEW_PREFIX.length());
                                    }
                                    return view;
                                })
                                .collect(Collectors.toList());
                        promise.fail(new DtmException(String.format("Views %s using the '%s' must be dropped first", viewNames, entity.getName().toUpperCase())));
                    }
                })
                .onFailure(promise::fail));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    @Override
    public List<PostSqlActionType> getPostActions() {
        return Arrays.asList(PostSqlActionType.PUBLISH_STATUS, PostSqlActionType.UPDATE_INFORMATION_SCHEMA);
    }
}
