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
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.eddl.DropDatabase;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.core.ddl.dto.DdlType.DROP_SCHEMA;

@Slf4j
@Component
public class DropSchemaDdlExecutor extends QueryResultDdlExecutor {
    private final CacheService<String, HotDelta> hotDeltaCacheService;
    private final CacheService<String, OkDelta> okDeltaCacheService;
    private final CacheService<EntityKey, Entity> entityCacheService;
    private final DatamartDao datamartDao;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public DropSchemaDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                 @Qualifier("hotDeltaCacheService") CacheService<String, HotDelta> hotDeltaCacheService,
                                 @Qualifier("okDeltaCacheService") CacheService<String, OkDelta> okDeltaCacheService,
                                 @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                 ServiceDbFacade serviceDbFacade,
                                 EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        super(metadataExecutor, serviceDbFacade);
        this.hotDeltaCacheService = hotDeltaCacheService;
        this.okDeltaCacheService = okDeltaCacheService;
        this.entityCacheService = entityCacheService;
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return dropSchema(context);
    }

    private Future<QueryResult> dropSchema(DdlRequestContext context) {
        return Future.future(promise -> {
            String datamartName = ((DropDatabase) context.getSqlNode()).getName().getSimple();
            if (InformationSchemaView.SCHEMA_NAME.equalsIgnoreCase(datamartName)) {
                promise.fail(new DtmException("System database INFORMATION_SCHEMA is non-deletable"));
            } else {
                clearCacheByDatamartName(datamartName);
                context.getRequest().getQueryRequest().setDatamartMnemonic(datamartName);
                context.setDatamartName(datamartName);
                datamartDao.existsDatamart(datamartName)
                        .compose(isExists -> {
                            if (isExists) {
                                try {
                                    evictQueryTemplateCacheService.evictByDatamartName(datamartName);
                                    return dropDatamartInPlugins(context);
                                } catch (Exception e) {
                                    return Future.failedFuture(new DtmException("Evict cache error"));
                                }
                            } else {
                                return getNotExistsDatamartFuture(datamartName);
                            }
                        })
                        .compose(r -> dropDatamart(datamartName))
                        .onSuccess(success -> {
                            try {
                                evictQueryTemplateCacheService.evictByDatamartName(datamartName);
                                promise.complete(QueryResult.emptyResult());
                            } catch (Exception e) {
                                promise.fail(new DtmException("Evict cache error"));
                            }
                        })
                        .onFailure(promise::fail);
            }
        });
    }

    private void clearCacheByDatamartName(String schemaName) {
        entityCacheService.removeIf(ek -> ek.getDatamartName().equals(schemaName));
        hotDeltaCacheService.remove(schemaName);
        okDeltaCacheService.remove(schemaName);
    }

    private Future<Void> getNotExistsDatamartFuture(String schemaName) {
        return Future.failedFuture(new DatamartNotExistsException(schemaName));
    }

    private Future<Void> dropDatamartInPlugins(DdlRequestContext context) {
        try {
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(DROP_SCHEMA);
            log.debug("Delete physical objects in plugins for datamart: [{}]", context.getDatamartName());
            return metadataExecutor.execute(context);
        } catch (Exception e) {
            return Future.failedFuture(new DtmException("Error generating drop datamart request", e));
        }
    }

    private Future<Void> dropDatamart(String datamartName) {
        log.debug("Delete schema [{}] in data sources", datamartName);
        return datamartDao.deleteDatamart(datamartName)
                .onSuccess(success -> log.debug("Deleted datamart [{}] from datamart registry", datamartName));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }
}
