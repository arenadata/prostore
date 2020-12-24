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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.enrichment;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.AdbCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adb.service.SchemaExtender;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AdbQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private final AdbCalciteContextProvider contextProvider;
    private final SchemaExtender schemaExtender;
    private final QueryParserService queryParserService;
    private final QueryGenerator adbQueryGenerator;

    public AdbQueryEnrichmentServiceImpl(
            @Qualifier("adbCalciteDMLQueryParserService") QueryParserService queryParserService,
            AdbQueryGeneratorImpl adbQueryGeneratorimpl,
            AdbCalciteContextProvider contextProvider,
            @Qualifier("adbSchemaExtender") SchemaExtender schemaExtender
    ) {
        this.queryParserService = queryParserService;
        this.adbQueryGenerator = adbQueryGeneratorimpl;
        this.contextProvider = contextProvider;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> asyncHandler) {
        queryParserService.parse(new QueryParserRequest(request.getQueryRequest(), request.getSchema()), ar -> {
            if (ar.succeeded()) {
                val parserResponse = ar.result();
                contextProvider.enrichContext(parserResponse.getCalciteContext(),
                        generatePhysicalSchemas(request.getSchema()));
                // form a new sql query
                adbQueryGenerator.mutateQuery(parserResponse.getRelNode(),
                        parserResponse.getQueryRequest().getDeltaInformations(),
                        parserResponse.getCalciteContext(),
                        enrichedQueryResult -> {
                            if (enrichedQueryResult.succeeded()) {
                                log.trace("Request generated: {}", enrichedQueryResult.result());
                                asyncHandler.handle(Future.succeededFuture(enrichedQueryResult.result()));
                            } else {
                                log.debug("Error while enriching request", enrichedQueryResult.cause());
                                asyncHandler.handle(Future.failedFuture(enrichedQueryResult.cause()));
                            }
                        });
            } else {
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }
        });

    }

    private List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas) {
        return logicalSchemas.stream().map(schemaExtender::createPhysicalSchema).collect(Collectors.toList());
    }
}
