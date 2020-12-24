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
package io.arenadata.dtm.query.execution.core.service.eddl.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlAction;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlExecutor;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.plugin.api.eddl.EddlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.EddlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("coreEddlService")
@Slf4j
public class EddlServiceImpl implements EddlService<QueryResult> {

    private final EddlQueryParamExtractor paramExtractor;
    private final Map<EddlAction, EddlExecutor> executors;
    private final MetricsService<RequestMetrics> metricsService;

    @Autowired
    public EddlServiceImpl(EddlQueryParamExtractor paramExtractor,
                           List<EddlExecutor> eddlExecutors,
                           MetricsService<RequestMetrics> metricsService) {
        this.paramExtractor = paramExtractor;
        this.executors = eddlExecutors.stream()
                .collect(Collectors.toMap(EddlExecutor::getAction, it -> it));
        this.metricsService = metricsService;
    }

    @Override
    public void execute(EddlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        extractQuery(context)
                .compose(eddlQuery -> sendMetricsAndExecute(context, eddlQuery))
                .onComplete(execHandler -> {
                    if (execHandler.succeeded()) {
                        asyncResultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                    } else {
                        log.error(execHandler.cause().getMessage());
                        asyncResultHandler.handle(Future.failedFuture(execHandler.cause()));
                    }
                });
    }

    private Future<EddlQuery> extractQuery(EddlRequestContext context) {
        return Future.future(promise -> paramExtractor.extract(context.getRequest().getQueryRequest(), promise));
    }

    private Future<Void> sendMetricsAndExecute(EddlRequestContext context, EddlQuery eddlQuery) {
        return metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA, SqlProcessingType.EDDL, context.getMetrics())
                .compose(v -> execute(context, eddlQuery));
    }

    private Future<Void> execute(EddlRequestContext context, EddlQuery eddlQuery) {
        return Future.future((Promise<Void> promise) -> {
            executors.get(eddlQuery.getAction()).execute(eddlQuery,
                    metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                            SqlProcessingType.EDDL,
                            context.getMetrics(),
                            promise));
        });
    }

}
