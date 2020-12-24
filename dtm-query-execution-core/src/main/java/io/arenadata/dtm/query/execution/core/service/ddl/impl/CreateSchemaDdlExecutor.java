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
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
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

import static io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType.CREATE_SCHEMA;

@Slf4j
@Component
public class CreateSchemaDdlExecutor extends QueryResultDdlExecutor {

    private final DatamartDao datamartDao;

    @Autowired
    public CreateSchemaDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                   ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            String schemaName = ((SqlCreateDatabase) context.getQuery()).getName().names.get(0);
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(CREATE_SCHEMA);
            context.setDatamartName(schemaName);
            createDatamartIfNotExists(context, handler);
        } catch (Exception e) {
            log.error("Error creating datamart!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private void createDatamartIfNotExists(DdlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        datamartDao.existsDatamart(context.getDatamartName())
            .compose(isExists -> isExists ? getDatamarAlreadyExistsFuture(context) : createDatamartInPlugins(context))
            .compose(v -> datamartDao.createDatamart(context.getDatamartName()))
            .onSuccess(success -> {
                log.debug("Datamart [{}] successfully created", context.getDatamartName());
                resultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            })
            .onFailure(error -> {
                log.error("Error creating datamart [{}]!", context.getDatamartName(), error);
                resultHandler.handle(Future.failedFuture(error));
            });
    }

    private Future<Void> getDatamarAlreadyExistsFuture(DdlRequestContext context) {
        return Future.failedFuture(new DatamartAlreadyExistsException(context.getDatamartName()));
    }

    private Future<Void> createDatamartInPlugins(DdlRequestContext context) {
        return Future.future((Promise<Void> promise) -> metadataExecutor.execute(context, promise))
            .onFailure(fail -> log.error("Error creating schema [{}] in data sources!", context.getDatamartName(), fail));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }
}
