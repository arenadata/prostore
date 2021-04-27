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
package io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.SchemaExtender;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.dto.EnrichQueryRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    public AdbQueryEnrichmentServiceImpl(
            @Qualifier("adbCalciteDMLQueryParserService") QueryParserService queryParserService,
            AdbQueryGeneratorImpl adbQueryGeneratorimpl,
            AdbCalciteContextProvider contextProvider,
            @Qualifier("adbSchemaExtender") SchemaExtender schemaExtender) {
        this.queryParserService = queryParserService;
        this.adbQueryGenerator = adbQueryGeneratorimpl;
        this.contextProvider = contextProvider;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request) {
        return queryParserService.parse(new QueryParserRequest(request.getQuery(), request.getSchema()))
                .map(response -> {
                    contextProvider.enrichContext(response.getCalciteContext(),
                            generatePhysicalSchemas(request.getSchema()));
                    return response;
                })
                .compose(queryParserResponse -> mutateQuery(queryParserResponse, request));
    }

    private Future<String> mutateQuery(QueryParserResponse response, EnrichQueryRequest request) {
        return Future.future(promise -> adbQueryGenerator.mutateQuery(response.getRelNode(),
                request.getDeltaInformations(),
                response.getCalciteContext())
                .onSuccess(result -> {
                    log.trace("Request generated: {}", result);
                    promise.complete(result);
                })
                .onFailure(promise::fail));
    }

    private List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas) {
        return logicalSchemas.stream()
                .map(schemaExtender::createPhysicalSchema)
                .collect(Collectors.toList());
    }
}
