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
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.plugin.api.request.UpsertRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.api.dml.LlwUtils.isValuesSqlNode;

@Component
@Slf4j
public class UpsertExecutor extends LlwExecutor {

    private static final List<String> SYSTEM_COLUMNS = Arrays.asList("sys_from", "sys_to", "sys_op");
    private final DataSourcePluginService pluginService;
    private final DeltaServiceDao deltaServiceDao;

    public UpsertExecutor(DataSourcePluginService pluginService,
                          ServiceDbFacade serviceDbFacade,
                          RestoreStateService restoreStateService) {
        super(serviceDbFacade.getServiceDbDao().getEntityDao(),
                pluginService,
                serviceDbFacade.getDeltaServiceDao(),
                restoreStateService);
        this.pluginService = pluginService;
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        return validateSqlNode(context)
                .compose(ignored -> getDestinationEntity(context))
                .compose(this::validateEntityType)
                .compose(entity -> validateColumns(context.getSqlNode(), entity))
                .compose(this::checkConfiguration)
                .compose(entity -> deltaServiceDao.getDeltaHot(context.getRequest().getQueryRequest().getDatamartMnemonic())
                        .compose(ignored -> deltaServiceDao.writeNewOperation(createDeltaOp(context, entity)))
                        .map(sysCn -> buildRequest(context, sysCn, entity)))
                .compose(llwRequest -> runUpsert(context, llwRequest))
                .map(QueryResult.emptyResult());
    }

    private Future<Entity> validateColumns(SqlNode sqlNode, Entity destination) {
        val insertNode = (SqlInsert) sqlNode;
        if (insertNode.getTargetColumnList() == null) {
            return Future.succeededFuture(destination);
        }
        val targetColumnNames = insertNode.getTargetColumnList().getList().stream()
                .map(node -> ((SqlIdentifier) node).getSimple())
                .collect(Collectors.toList());
        val containsSystemColumns = targetColumnNames.stream().anyMatch(SYSTEM_COLUMNS::contains);
        if (containsSystemColumns) {
            return Future.failedFuture(new ValidationDtmException(String.format("Columns [%s] is forbidden in UPSERT query", String.join(", ", SYSTEM_COLUMNS))));
        }
        val notNullableFields = EntityFieldUtils.getNotNullableFields(destination).stream()
                .map(EntityField::getName)
                .collect(Collectors.toList());
        if (!targetColumnNames.containsAll(notNullableFields)) {
            return Future.failedFuture(new ValidationDtmException(String.format("NOT NULL constraint failed. Some non-nullable columns [%s] are not set",
                    String.join(", ", notNullableFields))));
        }
        val entityColumnNames = EntityFieldUtils.getFieldNames(destination);
        targetColumnNames.removeAll(entityColumnNames);
        if (!targetColumnNames.isEmpty()) {
            return Future.failedFuture(new ValidationDtmException(String.format("Columns [%s] doesn't exist in entity %s",
                    String.join(", ", targetColumnNames), destination.getNameWithSchema())));
        }
        return Future.succeededFuture(destination);
    }

    private Future<Void> validateSqlNode(DmlRequestContext context) {
        if (!(context.getSqlNode() instanceof SqlInsert)) {
            return Future.failedFuture(new ValidationDtmException("Unsupported sql node"));
        }

        val originalSqlInsert = (SqlInsert) context.getSqlNode();
        if (!originalSqlInsert.isUpsert()) {
            return Future.failedFuture(new ValidationDtmException("Not upsert operation."));
        }

        if (!isValuesSqlNode(originalSqlInsert.getSource())) {
            return Future.failedFuture(new ValidationDtmException("Only VALUES source is allowed"));
        }

        return Future.succeededFuture();
    }

    private UpsertRequest buildRequest(DmlRequestContext context, Long sysCn, Entity entity) {
        val uuid = context.getRequest().getQueryRequest().getRequestId();
        val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val env = context.getEnvName();
        val params = context.getRequest().getQueryRequest().getParameters();
        return new UpsertRequest(uuid, env, datamart, sysCn, entity, (SqlInsert) context.getSqlNode(), params);
    }

    private Future<Void> runUpsert(DmlRequestContext context, UpsertRequest upsertRequest) {
        List<Future> futures = new ArrayList<>();
        upsertRequest.getEntity().getDestination().forEach(destination ->
                futures.add(pluginService.upsert(destination, context.getMetrics(), upsertRequest)));

        log.info("Executing LL-W request: {}", upsertRequest);

        return handleLlw(futures, upsertRequest);
    }

    @Override
    public DmlType getType() {
        return DmlType.UPSERT;
    }

}
