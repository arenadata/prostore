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
package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.*;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class LlrDmlExecutor implements DmlExecutor<QueryResult> {

    private final DataSourcePluginService dataSourcePluginService;
    private final TargetDatabaseDefinitionService targetDatabaseDefinitionService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final LogicViewReplacer logicViewReplacer;
    private final ColumnMetadataService columnMetadataService;
    private final InformationSchemaExecutor informationSchemaExecutor;
    private final InformationSchemaDefinitionService informationSchemaDefinitionService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final MetricsService<RequestMetrics> metricsService;

    @Autowired
    public LlrDmlExecutor(DataSourcePluginService dataSourcePluginService,
                          TargetDatabaseDefinitionService targetDatabaseDefinitionService,
                          DeltaQueryPreprocessor deltaQueryPreprocessor,
                          LogicViewReplacer logicViewReplacer,
                          ColumnMetadataService columnMetadataService,
                          InformationSchemaExecutor informationSchemaExecutor,
                          InformationSchemaDefinitionService informationSchemaDefinitionService,
                          LogicalSchemaProvider logicalSchemaProvider,
                          MetricsService<RequestMetrics> metricsService) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.targetDatabaseDefinitionService = targetDatabaseDefinitionService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.logicViewReplacer = logicViewReplacer;
        this.informationSchemaExecutor = informationSchemaExecutor;
        this.columnMetadataService = columnMetadataService;
        this.informationSchemaDefinitionService = informationSchemaDefinitionService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.metricsService = metricsService;
    }

    @Override
    public void execute(DmlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        try {
            val queryRequest = context.getRequest().getQueryRequest();
            val sourceRequest = new QuerySourceRequest(queryRequest, queryRequest.getSourceType());
            logicViewReplace(sourceRequest.getQueryRequest())
                    .compose(deltaQueryPreprocessor::process)
                    .map(request -> {
                        sourceRequest.setQueryRequest(request);
                        return request;
                    })
                    .compose(v -> initLogicalSchema(sourceRequest))
                    .compose(this::initColumnMetaData)
                    .compose(request -> executeRequest(request, context))
                    .onComplete(asyncResultHandler);
        } catch (Exception e) {
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }

    private Future<QueryRequest> logicViewReplace(QueryRequest request) {
        return Future.future(p -> logicViewReplacer.replace(request.getSql(), request.getDatamartMnemonic(), ar -> {
            if (ar.succeeded()) {
                QueryRequest withoutViewsRequest = request.copy();
                withoutViewsRequest.setSql(ar.result());
                p.complete(withoutViewsRequest);
            } else {
                p.fail(ar.cause());
            }
        }));
    }

    private Future<QuerySourceRequest> initLogicalSchema(QuerySourceRequest sourceRequest) {
        return getLogicalSchema(sourceRequest)
                .map(logicalSchema -> {
                    sourceRequest.setLogicalSchema(logicalSchema);
                    return sourceRequest;
                });
    }

    private Future<List<Datamart>> getLogicalSchema(QuerySourceRequest sourceRequest) {
        return Future.future((Promise<List<Datamart>> promise) -> {
            final QueryRequest queryRequest = sourceRequest.getQueryRequest();
            logicalSchemaProvider.getSchema(queryRequest, promise);
        });
    }

    private Future<QuerySourceRequest> initColumnMetaData(QuerySourceRequest request) {
        return Future.future(p -> {
            val parserRequest = new QueryParserRequest(request.getQueryRequest(), request.getLogicalSchema());
            columnMetadataService.getColumnMetadata(parserRequest, ar -> {
                if (ar.succeeded()) {
                    request.setMetadata(ar.result());
                    p.complete(request);
                } else {
                    p.fail(ar.cause());
                }
            });
        });
    }

    private Future<QueryResult> executeRequest(QuerySourceRequest sourceRequest,
                                               DmlRequestContext context) {
        return Future.future(promise -> {
            if (informationSchemaDefinitionService.isInformationSchemaRequest(sourceRequest)) {
                metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.LLR,
                        context.getMetrics())
                        .compose(v -> informationSchemaExecute(sourceRequest))
                        .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                SqlProcessingType.LLR,
                                context.getMetrics(),
                                promise));
            } else {
                defineTargetSourceType(sourceRequest)
                        .compose(querySourceRequest -> pluginExecute(querySourceRequest, context.getMetrics()))
                        .onComplete(promise);
            }
        });
    }

    private Future<QueryResult> informationSchemaExecute(QuerySourceRequest querySourceRequest) {
        return Future.future(p -> informationSchemaExecutor.execute(querySourceRequest, p));
    }

    private Future<QuerySourceRequest> defineTargetSourceType(QuerySourceRequest sourceRequest) {
        return Future.future(promise -> targetDatabaseDefinitionService.getTargetSource(sourceRequest, promise));
    }

    @SneakyThrows
    private Future<QueryResult> pluginExecute(QuerySourceRequest request, RequestMetrics requestMetrics) {
        return Future.future(p -> {
            dataSourcePluginService.llr(
                    request.getQueryRequest().getSourceType(),
                    new LlrRequestContext(
                            requestMetrics,
                            new LlrRequest(
                                    request.getQueryRequest(),
                                    request.getLogicalSchema(),
                                    request.getMetadata())
                    ),
                    p);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.SELECT;
    }

}
