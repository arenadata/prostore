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
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.base.service.converter.AdqmTemplateParameterConverter;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class AdqmLlrServiceTest {
    private final static String ENRICHED_QUERY = "enriched query";
    private final QueryEnrichmentService queryEnrichmentService = mock(QueryEnrichmentService.class);
    private final DatabaseExecutor executorService = mock(DatabaseExecutor.class);
    private final CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService = mock(CacheService.class);
    private final QueryTemplateExtractor templateExtractor = mock(QueryTemplateExtractor.class);
    private final SqlDialect sqlDialect = mock(SqlDialect.class);
    private final QueryParserService queryParserService = mock(QueryParserService.class);
    private final QueryParserResponse parserResponse = mock(QueryParserResponse.class);
    private final AdqmValidationService adqmValidationService = mock(AdqmValidationService.class);
    private final AdqmLlrService adqmLlrService = new AdqmLlrService(queryEnrichmentService, executorService,
            queryCacheService, templateExtractor, sqlDialect, queryParserService, new AdqmTemplateParameterConverter(), adqmValidationService);

    @BeforeEach
    void setUp() {
        when(queryCacheService.get(any())).thenReturn(null);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(queryEnrichmentService.enrich(any(), any())).thenReturn(Future.succeededFuture(ENRICHED_QUERY));
        when(templateExtractor.extract(anyString(), any()))
                .thenReturn(new QueryTemplateResult("", null, Collections.emptyList()));
        when(queryCacheService.put(any(), any()))
                .thenReturn(Future.succeededFuture(QueryTemplateValue.builder().build()));
        HashMap<String, Object> result = new HashMap<>();
        result.put("column", "value");
        when(executorService.executeWithParams(any(), any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));
    }

    @Test
    void testExecuteWithoutCacheSuccess() {
        List<ColumnMetadata> metadata = Collections.singletonList(ColumnMetadata.builder().build());
        UUID requestId = UUID.randomUUID();
        SqlNode sqlNode = mock(SqlNode.class);
        SqlString sqlString = mock(SqlString.class);
        LlrRequest request = LlrRequest.builder()
                .requestId(requestId)
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .metadata(metadata)
                .sourceQueryTemplateResult(new QueryTemplateResult("", null, Collections.emptyList()))
                .build();
        when(sqlString.getSql()).thenReturn(ENRICHED_QUERY);
        when(sqlNode.toSqlString(any(SqlDialect.class))).thenReturn(sqlString);
        when(templateExtractor.extract(any(SqlNode.class)))
                .thenReturn(new QueryTemplateResult(ENRICHED_QUERY, sqlNode, Collections.emptyList()));
        when(templateExtractor.extract(anyString()))
                .thenReturn(new QueryTemplateResult(ENRICHED_QUERY, sqlNode, Collections.emptyList()));
        when(templateExtractor.enrichTemplate(any())).thenReturn(sqlNode);
        adqmLlrService.execute(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals("value", ar.result().getResult().get(0).get("column"));
                    assertEquals(metadata, ar.result().getMetadata());
                    assertEquals(requestId, ar.result().getRequestId());
                    verify(executorService, times(1)).executeWithParams(eq(ENRICHED_QUERY), eq(null), eq(metadata));
                });
    }

    @Test
    void testEnrichQuerySuccess() {
        LlrRequest request = LlrRequest.builder().build();
        adqmLlrService.enrichQuery(request, null)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(ENRICHED_QUERY, ar.result());
                    verify(queryEnrichmentService, times(1)).enrich(any(), any());
                });
    }

    @Test
    void testQueryExecuteSuccess() {
        adqmLlrService.queryExecute("", null, Collections.emptyList())
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals("value", ar.result().get(0).get("column"));
                    verify(executorService, times(1)).executeWithParams(any(),
                            eq(null),
                            eq(Collections.emptyList()));
                });
    }
}
