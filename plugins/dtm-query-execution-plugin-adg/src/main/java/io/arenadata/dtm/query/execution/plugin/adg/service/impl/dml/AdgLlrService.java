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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl.dml;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("adgLlrService")
@Slf4j
public class AdgLlrService implements LlrService<QueryResult> {

    private final QueryEnrichmentService queryEnrichmentService;
    private final QueryExecutorService executorService;


    @Autowired
    public AdgLlrService(QueryEnrichmentService queryEnrichmentService,
                         QueryExecutorService executorService) {
        this.queryEnrichmentService = queryEnrichmentService;
        this.executorService = executorService;
    }

    @Override
    public void execute(LlrRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        LlrRequest request = context.getRequest();
        EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema());
        queryEnrichmentService.enrich(enrichQueryRequest, enrich -> {
            if (enrich.succeeded()) {
                executorService.execute(enrich.result(), request.getMetadata(), exec -> {
                    if (exec.succeeded()) {
                        handler.handle(Future.succeededFuture(
                            QueryResult.builder()
                                .requestId(request.getQueryRequest().getRequestId())
                                .metadata(request.getMetadata())
                                .result(exec.result())
                                .build()));
                    } else {
                        log.error("Request execution error {}", request, exec.cause());
                        handler.handle(Future.failedFuture(exec.cause()));
                    }
                });
            } else {
                log.error("Error while enriching request {}", request);
                handler.handle(Future.failedFuture(enrich.cause()));
            }
        });
    }

}
