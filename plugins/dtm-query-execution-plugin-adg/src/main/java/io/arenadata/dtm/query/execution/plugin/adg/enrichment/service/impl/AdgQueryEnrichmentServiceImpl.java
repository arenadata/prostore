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
package io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.SchemaExtender;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AdgQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private final AdgCalciteContextProvider contextProvider;
    private final QueryParserService queryParserService;
    private final QueryGenerator adgQueryGenerator;
    private final SchemaExtender schemaExtender;

    @Autowired
    public AdgQueryEnrichmentServiceImpl(
            @Qualifier("adgCalciteDMLQueryParserService") QueryParserService queryParserService,
            AdgCalciteContextProvider contextProvider,
            @Qualifier("adgQueryGenerator") QueryGenerator adgQueryGenerator, SchemaExtender schemaExtender) {
        this.contextProvider = contextProvider;
        this.queryParserService = queryParserService;
        this.adgQueryGenerator = adgQueryGenerator;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request) {
        return queryParserService.parse(new QueryParserRequest(request.getQuery(), request.getSchema()))
                .compose(parsedQuery -> modifyQuery(parsedQuery, request));
    }

    private Future<String> modifyQuery(QueryParserResponse parsedQuery,
                                       EnrichQueryRequest request) {
        return Future.future(promise -> {
            contextProvider.enrichContext(parsedQuery.getCalciteContext(),
                    generatePhysicalSchema(request.getSchema(), request.getEnvName()));
            // form a new sql query
            adgQueryGenerator.mutateQuery(parsedQuery.getRelNode(),
                    request.getDeltaInformations(),
                    parsedQuery.getCalciteContext(),
                    request)
                    .onSuccess(enrichResult -> {
                        log.debug("Request generated: {}", enrichResult);
                        promise.complete(enrichResult);
                    })
                    .onFailure(promise::fail);
        });
    }

    private List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, String envName) {
        return logicalSchemas.stream()
                .map(ls -> schemaExtender.createPhysicalSchema(ls, envName))
                .collect(Collectors.toList());
    }
}
