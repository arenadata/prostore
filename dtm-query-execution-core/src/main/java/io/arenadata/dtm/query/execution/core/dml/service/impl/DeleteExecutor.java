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
package io.arenadata.dtm.query.execution.core.dml.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.request.DeleteRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDelete;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class DeleteExecutor extends LlwExecutor {

    private final DataSourcePluginService pluginService;
    private final DeltaServiceDao deltaServiceDao;
    private final LogicalSchemaProvider logicalSchemaProvider;

    public DeleteExecutor(DataSourcePluginService pluginService,
                          ServiceDbFacade serviceDbFacade,
                          RestoreStateService restoreStateService,
                          LogicalSchemaProvider logicalSchemaProvider) {
        super(serviceDbFacade.getServiceDbDao().getEntityDao(),
                pluginService,
                serviceDbFacade.getDeltaServiceDao(),
                restoreStateService);
        this.pluginService = pluginService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        String datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return validate(context)
                .compose(ignored -> getDestinationEntity(context))
                .compose(this::validateEntityType)
                .compose(this::checkConfiguration)
                .compose(entity -> deltaServiceDao.getDeltaHot(datamart)
                        .compose(ignored -> deltaServiceDao.getDeltaOk(datamart))
                        .compose(okDelta -> {
                            if (okDelta == null) {
                                return handleDeleteWhenDatamartHasNoData();
                            }
                            return logicalSchemaProvider.getSchemaFromQuery(context.getSqlNode(), datamart)
                                    .compose(datamarts -> deltaServiceDao.writeNewOperation(createDeltaOp(context, entity))
                                            .map(sysCn -> buildRequest(context, entity, sysCn, okDelta.getCnTo(), datamarts)))
                                    .compose(deleteRequest -> runDelete(context, deleteRequest));
                        }))
                .map(QueryResult.emptyResult());
    }

    private DeleteRequest buildRequest(DmlRequestContext context,
                                       Entity entity,
                                       Long sysCn,
                                       Long cnTo,
                                       List<Datamart> datamarts) {
        val uuid = context.getRequest().getQueryRequest().getRequestId();
        val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val env = context.getEnvName();
        val parameters = context.getRequest().getQueryRequest().getParameters();

        return new DeleteRequest(uuid, env, datamart, entity, (SqlDelete) context.getSqlNode(), sysCn, cnTo, datamarts, parameters);
    }

    private Future<Void> validate(DmlRequestContext context) {
        if (!(context.getSqlNode() instanceof SqlDelete)) {
            return Future.failedFuture(new DtmException("Unsupported sql node"));
        }

        return Future.succeededFuture();
    }

    private Future<Void> handleDeleteWhenDatamartHasNoData() {
        return Future.succeededFuture();
    }

    private Future<Void> runDelete(DmlRequestContext context, DeleteRequest request) {
        List<Future> futures = new ArrayList<>();
        request.getEntity().getDestination().forEach(destination ->
                futures.add(pluginService.delete(destination, context.getMetrics(), request)));

        log.info("Executing LL-W request: {}", request);

        return handleLlw(futures, request);
    }

    @Override
    public DmlType getType() {
        return DmlType.DELETE;
    }

}
