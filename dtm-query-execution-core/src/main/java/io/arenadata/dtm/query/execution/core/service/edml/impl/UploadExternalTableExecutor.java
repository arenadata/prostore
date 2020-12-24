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
package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction.UPLOAD;

@Service
@Slf4j
public class UploadExternalTableExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final DeltaServiceDao deltaServiceDao;
    private final Map<ExternalTableLocationType, EdmlUploadExecutor> executors;
    private final EdmlUploadFailedExecutor uploadFailedExecutor;
    private final DataSourcePluginService pluginService;
    private final LogicalSchemaProvider logicalSchemaProvider;

    @Autowired
    public UploadExternalTableExecutor(DeltaServiceDao deltaServiceDao,
                                       EdmlUploadFailedExecutor uploadFailedExecutor,
                                       List<EdmlUploadExecutor> uploadExecutors,
                                       DataSourcePluginService pluginService,
                                       LogicalSchemaProvider logicalSchemaProvider) {
        this.deltaServiceDao = deltaServiceDao;
        this.uploadFailedExecutor = uploadFailedExecutor;
        this.executors = uploadExecutors.stream()
                .collect(Collectors.toMap(EdmlUploadExecutor::getUploadType, it -> it));
        this.pluginService = pluginService;
        this.logicalSchemaProvider = logicalSchemaProvider;
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        isEntitySourceTypesExistsInConfiguration(context)
                .compose(v -> writeNewOperationIfNeeded(context, context.getSourceEntity()))
                .compose(v -> executeAndWriteOp(context))
                .compose(queryResult -> writeOpSuccess(context.getSourceEntity().getSchema(), context.getSysCn(), queryResult))
                .onComplete(resultHandler);
    }

    private Future<Void> isEntitySourceTypesExistsInConfiguration(EdmlRequestContext context) {
        final Set<SourceType> nonExistDestionationTypes = context.getDestinationEntity().getDestination().stream()
                .filter(type -> !pluginService.getSourceTypes().contains(type))
                .collect(Collectors.toSet());
        if (!nonExistDestionationTypes.isEmpty()) {
            final String failureMessage = String.format("Plugins: %s for the table [%s] datamart [%s] are not configured",
                    nonExistDestionationTypes,
                    context.getDestinationEntity().getName(),
                    context.getDestinationEntity().getSchema());
            log.error(failureMessage);
            return Future.failedFuture(failureMessage);
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Long> writeNewOperationIfNeeded(EdmlRequestContext context, Entity entity) {
        if (context.getSysCn() != null) {
            return Future.succeededFuture();
        } else {
            return Future.future(writePromise -> deltaServiceDao.writeNewOperation(createDeltaOp(context, entity))
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            long sysCn = ar.result();
                            context.setSysCn(sysCn);
                            writePromise.complete();
                        } else {
                            writePromise.fail(ar.cause());
                        }
                    }));
        }
    }

    private DeltaWriteOpRequest createDeltaOp(EdmlRequestContext context, Entity entity) {
        return DeltaWriteOpRequest.builder()
                .datamart(entity.getSchema())
                .tableName(context.getDestinationEntity().getName())
                .tableNameExt(entity.getName())
                .query(context.getSqlNode().toSqlString(SQL_DIALECT).toString())
                .build();
    }

    private Future<QueryResult> executeAndWriteOp(EdmlRequestContext context) {
        return Future.future(promise ->
                initLogicalSchema(context)
                .compose(ctx -> execute(context))
                        .onSuccess(promise::complete)
                        .onFailure(error -> {
                            log.error("Edml write operation error!", error);
                            deltaServiceDao.writeOperationError(context.getSourceEntity().getSchema(), context.getSysCn())
                                    .compose(v -> uploadFailedExecutor.execute(context))
                                    .onComplete(writeErrorOpAr -> {
                                        if (writeErrorOpAr.succeeded()) {
                                            promise.fail(error);
                                        } else {
                                            log.error("Can't write operation error!", writeErrorOpAr.cause());
                                            promise.fail(writeErrorOpAr.cause());
                                        }
                                    });
                        }));
    }

    private Future<QueryResult> execute(EdmlRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            if (ExternalTableLocationType.KAFKA == context.getSourceEntity().getExternalTableLocationType()) {
                executors.get(context.getSourceEntity().getExternalTableLocationType()).execute(context, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                });
            } else {
                log.error("Loading type {} not implemented", context.getSourceEntity().getExternalTableLocationType());
                promise.fail(new RuntimeException("Other download types are not yet implemented!"));
            }
        });
    }

    private Future<QueryResult> writeOpSuccess(String datamartName, Long sysCn, QueryResult result) {
        return Future.future(promise ->
                deltaServiceDao.writeOperationSuccess(datamartName, sysCn)
                        .onSuccess(v -> promise.complete(result))
                        .onFailure(promise::fail));
    }

    private Future<Void> initLogicalSchema(EdmlRequestContext context) {
        return Future.future(promise -> {
            logicalSchemaProvider.getSchema(context.getRequest().getQueryRequest(), ar -> {
                if (ar.succeeded()) {
                    final List<Datamart> logicalSchema = ar.result();
                    context.setLogicalSchema(logicalSchema);
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    @Override
    public EdmlAction getAction() {
        return UPLOAD;
    }
}
