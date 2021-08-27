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
package io.arenadata.dtm.query.execution.plugin.adb.dml.service;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dml.LlrPlanResult;
import io.arenadata.dtm.query.execution.plugin.api.dml.LlrEstimateUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.*;
import static java.util.Collections.singletonList;

@Slf4j
@Service("adbLlrService")
public class AdbLlrService extends QueryResultCacheableLlrService {
    private static final List<String> SYSTEM_FIELDS = Arrays.asList(SYS_FROM_ATTR, SYS_TO_ATTR, SYS_OP_ATTR);
    private final QueryEnrichmentService queryEnrichmentService;
    private final DatabaseExecutor queryExecutor;

    @Autowired
    public AdbLlrService(@Qualifier("adbQueryEnrichmentService") QueryEnrichmentService adbQueryEnrichmentService,
                         @Qualifier("adbQueryExecutor") DatabaseExecutor adbDatabaseExecutor,
                         @Qualifier("adbQueryTemplateCacheService")
                                 CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                         @Qualifier("adbQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                         @Qualifier("adbSqlDialect") SqlDialect sqlDialect,
                         @Qualifier("adbCalciteDMLQueryParserService") QueryParserService queryParserService) {
        super(queryCacheService, templateExtractor, sqlDialect, queryParserService);
        this.queryEnrichmentService = adbQueryEnrichmentService;
        this.queryExecutor = adbDatabaseExecutor;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                             QueryParameters queryParameters,
                                                             List<ColumnMetadata> metadata) {
        return queryExecutor.executeWithParams(enrichedQuery, queryParameters, metadata);
    }

    @Override
    protected Future<LlrPlanResult> estimateQueryExecute(String enrichedQuery, QueryParameters queryParameters) {
        return queryExecutor.executeWithParams("EXPLAIN (FORMAT JSON) " + enrichedQuery, queryParameters,
                        singletonList(LlrEstimateUtils.LLR_ESTIMATE_METADATA))
                .map(resultSet -> new LlrPlanResult(SourceType.ADB, LlrEstimateUtils.extractPlanJson(resultSet)));
    }

    @Override
    protected void validateQuery(QueryParserResponse parserResponse) {
    }

    @Override
    protected Future<String> enrichQuery(LlrRequest request, QueryParserResponse parserResponse) {
        return queryEnrichmentService.enrich(EnrichQueryRequest.builder()
                        .deltaInformations(request.getDeltaInformations())
                        .envName(request.getEnvName())
                        .query(request.getWithoutViewsQuery())
                        .schema(request.getSchema())
                        .build(),
                parserResponse);
    }

    @Override
    protected List<String> ignoredSystemFieldsInTemplate() {
        return SYSTEM_FIELDS;
    }
}
