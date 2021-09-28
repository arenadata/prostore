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
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.core.ddl.dto.DdlType.DROP_SCHEMA;

@Slf4j
@Component
public class DropSchemaExecutor extends QueryResultDdlExecutor {
    private final CacheService<String, HotDelta> hotDeltaCacheService;
    private final CacheService<String, OkDelta> okDeltaCacheService;
    private final CacheService<EntityKey, Entity> entityCacheService;
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    private final DatamartDao datamartDao;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public DropSchemaExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                              @Qualifier("hotDeltaCacheService") CacheService<String, HotDelta> hotDeltaCacheService,
                              @Qualifier("okDeltaCacheService") CacheService<String, OkDelta> okDeltaCacheService,
                              @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                              @Qualifier("materializedViewCacheService") CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService,
                              ServiceDbFacade serviceDbFacade,
                              EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        super(metadataExecutor, serviceDbFacade);
        this.hotDeltaCacheService = hotDeltaCacheService;
        this.okDeltaCacheService = okDeltaCacheService;
        this.entityCacheService = entityCacheService;
        this.materializedViewCacheService = materializedViewCacheService;
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            String datamartName = ((DropDatabase) context.getSqlNode()).getName().getSimple();
            clearCacheByDatamartName(datamartName);
            context.getRequest().getQueryRequest().setDatamartMnemonic(datamartName);
            context.setDatamartName(datamartName);
            datamartDao.existsDatamart(datamartName)
                    .compose(isExists -> {
                        if (isExists) {
                            try {
                                evictQueryTemplateCacheService.evictByDatamartName(datamartName);
                                context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
                                context.setDdlType(DROP_SCHEMA);
                                return executeRequest(context);
                            } catch (Exception e) {
                                return Future.failedFuture(new DtmException("Evict cache error", e));
                            }
                        } else {
                            return getNotExistsDatamartFuture(datamartName);
                        }
                    })
                    .compose(r -> dropDatamart(datamartName))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    private void clearCacheByDatamartName(String schemaName) {
        entityCacheService.removeIf(ek -> ek.getDatamartName().equals(schemaName));
        materializedViewCacheService.forEach(((entityKey, cacheValue) -> {
            if (entityKey.getDatamartName().equals(schemaName)) {
                cacheValue.markForDeletion();
            }
        }));
        hotDeltaCacheService.remove(schemaName);
        okDeltaCacheService.remove(schemaName);
    }

    private Future<Void> getNotExistsDatamartFuture(String schemaName) {
        return Future.failedFuture(new DatamartNotExistsException(schemaName));
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
