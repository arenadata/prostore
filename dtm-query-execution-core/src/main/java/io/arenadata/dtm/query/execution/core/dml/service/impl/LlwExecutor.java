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

import io.arenadata.dtm.common.dto.TableInfo;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.service.DmlExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.plugin.api.request.LlwRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public abstract class LlwExecutor implements DmlExecutor<QueryResult> {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);

    private final EntityDao entityDao;
    private final DataSourcePluginService pluginService;
    private final DeltaServiceDao deltaServiceDao;
    private final RestoreStateService restoreStateService;

    public LlwExecutor(EntityDao entityDao,
                       DataSourcePluginService pluginService,
                       DeltaServiceDao deltaServiceDao,
                       RestoreStateService restoreStateService) {
        this.entityDao = entityDao;
        this.pluginService = pluginService;
        this.deltaServiceDao = deltaServiceDao;
        this.restoreStateService = restoreStateService;
    }

    protected Future<Entity> getDestinationEntity(DmlRequestContext context) {
        String defaultDatamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val tableAndSnapshots = new SqlSelectTree(context.getSqlNode()).findAllTableAndSnapshots();
        val tableInfos = tableAndSnapshots
                .stream()
                .map(item -> new TableInfo(item.tryGetSchemaName().orElse(defaultDatamartMnemonic),
                        item.tryGetTableName().orElseThrow(() ->
                                new DtmException(String.format("Can't determine table from query [%s]", context.getRequest().getQueryRequest().getSql())))))
                .collect(Collectors.toList());
        val destination = tableInfos.get(0);
        return entityDao.getEntity(destination.getSchemaName(), destination.getTableName());
    }

    protected Future<Entity> validateEntityType(Entity entity) {
        if (entity.getEntityType() != EntityType.TABLE) {
            return Future.failedFuture(new DtmException("Forbidden. Write operations allowed for logical tables only."));
        }
        return Future.succeededFuture(entity);
    }

    protected Future<Entity> checkConfiguration(Entity destination) {
        final Set<SourceType> nonExistDestinationTypes = destination.getDestination().stream()
                .filter(type -> !pluginService.hasSourceType(type))
                .collect(Collectors.toSet());
        if (!nonExistDestinationTypes.isEmpty()) {
            final String failureMessage = String.format("Plugins: %s for the table [%s] datamart [%s] are not configured",
                    nonExistDestinationTypes,
                    destination.getName(),
                    destination.getSchema());
            return Future.failedFuture(new DtmException(failureMessage));
        } else {
            return Future.succeededFuture(destination);
        }
    }

    protected DeltaWriteOpRequest createDeltaOp(DmlRequestContext context, Entity entity) {
        return DeltaWriteOpRequest.builder()
                .datamart(entity.getSchema())
                .tableName(entity.getName())
                .query(context.getSqlNode().toSqlString(SQL_DIALECT).toString())
                .build();
    }

    protected Future<Void> handleLlw(List<Future> futures, LlwRequest request) {
        return Future.future(promise -> {
            CompositeFuture.join(futures)
                    .compose(ignored -> deltaServiceDao.writeOperationSuccess(request.getDatamartMnemonic(), request.getSysCn()))
                    .onSuccess(ar -> {
                        log.info("LL-W request completed successfully for datamart {} and sys_cn {}", request.getDatamartMnemonic(), request.getSysCn());
                        promise.complete();
                    })
                    .onFailure(error -> {
                        log.error("LL-W request failed: {}. Cleaning up", request, error);
                        deltaServiceDao.writeOperationError(request.getDatamartMnemonic(), request.getSysCn())
                                .compose(ignored -> restoreStateService.restoreErase(request.getDatamartMnemonic()))
                                .onComplete(rollbackAr -> {
                                    if (rollbackAr.failed()) {
                                        log.error("Rollback for LL-W [{}] failed", request, rollbackAr.cause());
                                    }

                                    if (error instanceof DtmException) {
                                        promise.fail(error);
                                    } else if (error.getMessage() != null) {
                                        promise.fail(new DtmException("Unexpected error: " + error.getMessage(), error));
                                    } else {
                                        promise.fail(new DtmException("Unexpected error", error));
                                    }
                                });
                    });
        });
    }

}
