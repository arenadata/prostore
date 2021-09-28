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
package io.arenadata.dtm.query.execution.plugin.adqm.dml.service;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dml.LlrPlanResult;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrValidationService;
import io.arenadata.dtm.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service("adqmLlrService")
@Slf4j
public class AdqmLlrService extends QueryResultCacheableLlrService {
    private static final List<String> SYSTEM_FIELDS = new ArrayList<>(Constants.SYSTEM_FIELDS);
    private static final LlrPlanResult LLR_EMPTY_ESTIMATE_RESULT = new LlrPlanResult(SourceType.ADQM);
    private final QueryEnrichmentService queryEnrichmentService;
    private final DatabaseExecutor executorService;
    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final LlrValidationService adqmValidationService;

    @Autowired
    public AdqmLlrService(@Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                          @Qualifier("adqmQueryExecutor") DatabaseExecutor adqmQueryExecutor,
                          @Qualifier("adqmQueryTemplateCacheService")
                                  CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                          @Qualifier("adqmQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                          @Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                          @Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService,
                          @Qualifier("adqmTemplateParameterConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                          @Qualifier("adqmValidationService") LlrValidationService adqmValidationService) {
        super(queryCacheService, templateExtractor, sqlDialect, queryParserService);
        this.queryEnrichmentService = queryEnrichmentService;
        this.executorService = adqmQueryExecutor;
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.adqmValidationService = adqmValidationService;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                             QueryParameters queryParameters,
                                                             List<ColumnMetadata> metadata) {
        return executorService.executeWithParams(enrichedQuery, queryParameters, metadata);
    }

    @Override
    protected Future<LlrPlanResult> estimateQueryExecute(String enrichedQuery, QueryParameters queryParameters) {
        return Future.succeededFuture(LLR_EMPTY_ESTIMATE_RESULT);
    }

    @Override
    protected QueryParameters getExtendedQueryParameters(LlrRequest request) {
        QueryParameters queryParameters = request.getParameters();
        // For adqm enrichment query we have to create x2 params values and their types (excluding LIMIT/OFSSET params)
        if (queryParameters != null) {
            var holdParameters = 0;
            if (request.getOriginalQuery().getClass() == LimitableSqlOrderBy.class) {
                val originalQuery = (LimitableSqlOrderBy) request.getOriginalQuery();
                if (originalQuery.fetch != null && originalQuery.fetch.getClass() == SqlDynamicParam.class) {
                    holdParameters++;
                }

                if (originalQuery.offset != null && originalQuery.offset.getClass() == SqlDynamicParam.class) {
                    holdParameters++;
                }
            }

            val queryParamValues = queryParameters.getValues();
            val queryParamTypes = queryParameters.getTypes();
            val values = new ArrayList<>(queryParamValues.subList(0, queryParamValues.size() - holdParameters));
            val types = new ArrayList<>(queryParamTypes.subList(0, queryParamTypes.size() - holdParameters));
            values.addAll(queryParamValues);
            types.addAll(queryParamTypes);
            return new QueryParameters(values, types);
        } else {
            return null;
        }
    }

    @Override
    protected void validateQuery(QueryParserResponse parserResponse) {
        adqmValidationService.validate(parserResponse);
    }

    @Override
    protected Future<String> enrichQuery(LlrRequest request, QueryParserResponse parserResponse) {
        return queryEnrichmentService.enrich(EnrichQueryRequest.builder()
                        .query(request.getWithoutViewsQuery())
                        .deltaInformations(request.getDeltaInformations())
                        .envName(request.getEnvName())
                        .schema(request.getSchema())
                        .build(),
                parserResponse);
    }

    @Override
    protected List<SqlNode> convertParams(List<SqlNode> params, List<SqlTypeName> parameterTypes) {
        return pluginSpecificLiteralConverter.convert(params, parameterTypes);
    }

    @Override
    protected List<String> ignoredSystemFieldsInTemplate() {
        return SYSTEM_FIELDS;
    }
}
