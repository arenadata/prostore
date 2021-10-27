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

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.plugin.api.dml.LlwUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.UpsertValuesRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class UpsertValuesExecutor extends UpsertExecutor<UpsertValuesRequest> {
    private final DataSourcePluginService pluginService;

    public UpsertValuesExecutor(DataSourcePluginService pluginService,
                                ServiceDbFacade serviceDbFacade,
                                RestoreStateService restoreStateService) {
        super(pluginService, serviceDbFacade, restoreStateService);
        this.pluginService = pluginService;
    }

    @Override
    protected boolean isValidSource(SqlNode sqlInsert) {
        return LlwUtils.isValuesSqlNode(sqlInsert);
    }

    @Override
    protected Future<UpsertValuesRequest> buildRequest(DmlRequestContext context, Long sysCn, Entity entity) {
        val uuid = context.getRequest().getQueryRequest().getRequestId();
        val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val env = context.getEnvName();
        val params = context.getRequest().getQueryRequest().getParameters();
        return Future.succeededFuture(new UpsertValuesRequest(uuid, env, datamart, sysCn, entity, (SqlInsert) context.getSqlNode(), params));
    }

    @Override
    protected Future<?> runOperation(DmlRequestContext context, UpsertValuesRequest upsertRequest) {
        List<Future> futures = new ArrayList<>();
        upsertRequest.getEntity().getDestination().forEach(destination ->
                futures.add(pluginService.upsert(destination, context.getMetrics(), upsertRequest)));
        return CompositeFuture.join(futures);
    }

    @Override
    public DmlType getType() {
        return DmlType.UPSERT_VALUES;
    }
}
