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
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicatePart;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicates;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeTemplates;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.delta.exception.TableBlockedException;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.service.DmlExecutor;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public abstract class LlwExecutor implements DmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private static final Pattern VALUES_PATTERN = Pattern.compile("(?i)(UPSERT INTO .+ VALUES )(.+)");
    private static final SqlPredicates DYNAMIC_PARAM_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eq(SqlKind.DYNAMIC_PARAM))
            .build();

    private final EntityDao entityDao;
    private final DataSourcePluginService pluginService;
    private final DeltaServiceDao deltaServiceDao;
    private final RestoreStateService restoreStateService;

    protected LlwExecutor(EntityDao entityDao,
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

    protected Future<Long> produceOrResumeWriteOperation(DmlRequestContext context, Entity entity) {
        return Future.future(p -> {
            val writeOpRequest = createDeltaOp(context, entity);
            deltaServiceDao.writeNewOperation(writeOpRequest)
                    .onSuccess(p::complete)
                    .onFailure(t -> {
                        if (t instanceof TableBlockedException) {
                            deltaServiceDao.getDeltaWriteOperations(context.getRequest().getQueryRequest().getDatamartMnemonic())
                                    .map(writeOps -> findEqualWriteOp(writeOps, entity.getName(), writeOpRequest.getQuery(), t))
                                    .onComplete(p);
                            return;
                        }

                        p.fail(t);
                    });
        });
    }

    private Long findEqualWriteOp(List<DeltaWriteOp> writeOps, String tableName, String query, Throwable originalException) {
        return writeOps.stream()
                .filter(deltaWriteOp -> deltaWriteOp.getStatus() == WriteOperationStatus.EXECUTING.getValue())
                .filter(deltaWriteOp -> Objects.equals(deltaWriteOp.getTableName(), tableName))
                .filter(deltaWriteOp -> Objects.equals(query, deltaWriteOp.getQuery()))
                .findFirst()
                .map(DeltaWriteOp::getSysCn)
                .orElseThrow(() -> new DtmException("Table blocked and could not find equal writeOp for resume", originalException));
    }

    protected DeltaWriteOpRequest createDeltaOp(DmlRequestContext context, Entity entity) {
        return DeltaWriteOpRequest.builder()
                .datamart(entity.getSchema())
                .tableName(entity.getName())
                .query(hashQuery(context))
                .build();
    }

    private String hashQuery(DmlRequestContext context) {
        val parameters = context.getRequest().getQueryRequest().getParameters();
        val sqlNode = parameters != null ? SqlNodeUtil.copy(context.getSqlNode()) : context.getSqlNode();
        if (parameters != null) {
            val dynamicNodes = new SqlSelectTree(sqlNode)
                    .findNodes(DYNAMIC_PARAM_PREDICATE, false);
            for (int i = 0; i < dynamicNodes.size(); i++) {
                val treeNode = dynamicNodes.get(i);
                val value = parameters.getValues().get(i);
                val columnType = parameters.getTypes().get(i);
                treeNode.getSqlNodeSetter().accept(SqlNodeTemplates.literalForParameter(value, columnType));
            }
        }

        val query = sqlNode.toSqlString(SQL_DIALECT).toString()
                .replaceAll("\r\n|\r|\n", " ");

        val matcher = VALUES_PATTERN.matcher(query);
        if (matcher.matches()) {
            return matcher.group(1) + DigestUtils.md5Hex(matcher.group(2));
        }

        return query;
    }

    protected Future<Void> handleOperation(Future<?> llwFuture, long sysCn, String datamart, Entity entity) {
        return Future.future(promise -> {
            llwFuture.compose(ignored -> deltaServiceDao.writeOperationSuccess(datamart, sysCn))
                    .onSuccess(ar -> {
                        log.info("LL-W request succeeded [{}], sysCn: {}", entity.getNameWithSchema(), sysCn);
                        promise.complete();
                    })
                    .onFailure(error -> {
                        log.error("LL-W request failed [{}], sysCn: {}. Cleaning up", entity.getNameWithSchema(), sysCn, error);
                        deltaServiceDao.writeOperationError(datamart, sysCn)
                                .compose(ignored -> restoreStateService.restoreErase(datamart))
                                .onComplete(rollbackAr -> {
                                    if (rollbackAr.failed()) {
                                        log.error("Rollback for LL-W [{}] failed, sysCn: {}", entity.getNameWithSchema(), sysCn, rollbackAr.cause());
                                    }
                                    promise.fail(error);
                                });
                    });
        });
    }

}
