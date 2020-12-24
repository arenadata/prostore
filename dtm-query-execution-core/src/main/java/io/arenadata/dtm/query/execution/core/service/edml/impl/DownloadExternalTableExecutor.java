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

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.service.dml.LogicViewReplacer;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction.DOWNLOAD;

@Service
@Slf4j
public class DownloadExternalTableExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final Map<ExternalTableLocationType, EdmlDownloadExecutor> executors;
    private final LogicViewReplacer logicViewReplacer;

    @Autowired
    public DownloadExternalTableExecutor(LogicalSchemaProvider logicalSchemaProvider,
                                         DeltaQueryPreprocessor deltaQueryPreprocessor,
                                         List<EdmlDownloadExecutor> downloadExecutors,
                                         LogicViewReplacer logicViewReplacer) {
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.executors = downloadExecutors.stream().collect(Collectors.toMap(EdmlDownloadExecutor::getDownloadType, it -> it));
        this.logicViewReplacer = logicViewReplacer;
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        initDMLSubquery(context)
            .compose(v -> replaceView(context))
            .compose(v -> initLogicalSchema(context))
            .compose(v -> initDeltaInformation(context))
            .compose(v -> execute(context))
            .onComplete(resultHandler);
    }

    private Future<Object> replaceView(EdmlRequestContext context) {
        return Future.future(p -> logicViewReplacer.replace(context.getDmlSubquery(),
            context.getRequest().getQueryRequest().getDatamartMnemonic(), ar -> {
                if (ar.succeeded()) {
                    context.setDmlSubquery(ar.result());
                    p.complete();
                } else {
                    p.fail(ar.cause());
                }
            }));
    }

    private Future<Void> initDMLSubquery(EdmlRequestContext context) {
        return Future.future(promise -> {
            context.setDmlSubquery(context.getSqlNode().getSource().toSqlString(SQL_DIALECT).toString());
            promise.complete();
        });
    }

    private Future<Void> initLogicalSchema(EdmlRequestContext context) {
        return Future.future(promise -> {
            QueryRequest copy = context.getRequest().getQueryRequest().copy();
            copy.setSql(context.getDmlSubquery());
            logicalSchemaProvider.getSchema(copy, ar -> {
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

    private Future<QueryRequest> initDeltaInformation(EdmlRequestContext context) {
        return Future.future(promise -> {
            val copyRequest = context.getRequest().getQueryRequest().copy();
            copyRequest.setSql(context.getDmlSubquery());
            deltaQueryPreprocessor.process(copyRequest)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        final QueryRequest queryRequest = ar.result();
                        context.getRequest().setQueryRequest(queryRequest);
                        promise.complete(queryRequest);
                    } else {
                        promise.fail(ar.cause());
                    }
                });
        });
    }

    private Future<QueryResult> execute(EdmlRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            val destination = context.getDestinationEntity();
            if (ExternalTableLocationType.KAFKA == destination.getExternalTableLocationType()) {
                executors.get(destination.getExternalTableLocationType()).execute(context, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Mppr into table [{}] for dml query [{}] finished successfully",
                            destination.getName(), context.getDmlSubquery());
                        promise.complete(ar.result());
                    } else {
                        log.error("Error executing mppr into table [{}] for dml query [{}]",
                            destination.getName(),
                            context.getDmlSubquery());
                        promise.fail(ar.cause());
                    }
                });
            } else {
                log.error("Unload type {} not implemented", destination.getExternalTableLocationType());
                promise.fail(new RuntimeException("Other types of upload are not yet implemented!"));
            }
        });
    }

    @Override
    public EdmlAction getAction() {
        return DOWNLOAD;
    }
}
