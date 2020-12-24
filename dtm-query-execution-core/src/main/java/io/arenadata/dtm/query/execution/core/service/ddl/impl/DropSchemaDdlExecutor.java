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

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.eddl.DropDatabase;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.cache.impl.HotDeltaCacheService;
import io.arenadata.dtm.query.execution.core.service.cache.impl.OkDeltaCacheService;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType.DROP_SCHEMA;

@Slf4j
@Component
public class DropSchemaDdlExecutor extends QueryResultDdlExecutor {
    private final HotDeltaCacheService hotDeltaCacheService;
    private final OkDeltaCacheService okDeltaCacheService;
    private final EntityCacheService entityCacheService;
    private final DatamartDao datamartDao;

    @Autowired
    public DropSchemaDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                 HotDeltaCacheService hotDeltaCacheService,
                                 OkDeltaCacheService okDeltaCacheService,
                                 EntityCacheService entityCacheService,
                                 ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        this.hotDeltaCacheService = hotDeltaCacheService;
        this.okDeltaCacheService = okDeltaCacheService;
        this.entityCacheService = entityCacheService;
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            String schemaName = ((DropDatabase) context.getQuery()).getName().names.get(0);
            clearCacheByDatamartName(schemaName);
            context.getRequest().getQueryRequest().setDatamartMnemonic(schemaName);
            context.setDatamartName(schemaName);
            datamartDao.existsDatamart(schemaName)
                .compose(isExists -> isExists ? dropDatamartInPlugins(context) : getNotExistsDatamartFuture(schemaName))
                .compose(r -> dropDatamart(context))
                .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error deleting datamart!", e);
            handler.handle(Future.failedFuture(e));
        }
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
            return Future.future((Promise<Void> promise) -> metadataExecutor.execute(context, promise));
        } catch (Exception e) {
            log.error("Error in dropping schema [{}]", context.getDatamartName(), e);
            return Future.failedFuture(e);
        }
    }

    private Future<Void> dropDatamart(DdlRequestContext context) {
        log.debug("Deleted schema [{}] in data sources", context.getDatamartName());
        return datamartDao.deleteDatamart(context.getDatamartName())
            .onSuccess(success -> log.debug("Deleted datamart [{}] from datamart registry", context.getDatamartName()))
            .onFailure(error -> log.error("Error deleting datamart [{}] from datamart registry!", context.getDatamartName(), error));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }
}
