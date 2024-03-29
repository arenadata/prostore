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
package io.arenadata.dtm.query.execution.plugin.adp.enrichment.service;

import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.service.AdpCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.SchemaExtender;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service("adpQueryEnrichmentService")
@Slf4j
public class AdpQueryEnrichmentService implements QueryEnrichmentService {
    private final AdpCalciteContextProvider contextProvider;
    private final SchemaExtender schemaExtender;
    private final QueryGenerator adpQueryGenerator;

    @Autowired
    public AdpQueryEnrichmentService(
            @Qualifier("adpQueryGenerator") QueryGenerator adpQueryGenerator,
            AdpCalciteContextProvider contextProvider,
            @Qualifier("adpSchemaExtender") SchemaExtender schemaExtender) {
        this.adpQueryGenerator = adpQueryGenerator;
        this.contextProvider = contextProvider;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        contextProvider.enrichContext(parserResponse.getCalciteContext(),
                generatePhysicalSchemas(request.getSchema()));
        return mutateQuery(parserResponse, request);
    }

    @Override
    public Future<SqlNode> getEnrichedSqlNode(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        contextProvider.enrichContext(parserResponse.getCalciteContext(),
                generatePhysicalSchemas(request.getSchema()));
        return adpQueryGenerator.getMutatedSqlNode(parserResponse.getRelNode(),
                request.getDeltaInformations(),
                parserResponse.getCalciteContext(),
                null);
    }

    private Future<String> mutateQuery(QueryParserResponse response, EnrichQueryRequest request) {
        return Future.future(promise -> adpQueryGenerator.mutateQuery(response.getRelNode(),
                request.getDeltaInformations(),
                response.getCalciteContext(),
                null)
                .onSuccess(result -> {
                    log.trace("Request generated: {}", result);
                    promise.complete(result);
                })
                .onFailure(promise::fail));
    }

    private List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas) {
        return logicalSchemas.stream()
                .map(logicalSchema -> schemaExtender.createPhysicalSchema(logicalSchema, ""))
                .collect(Collectors.toList());
    }
}
