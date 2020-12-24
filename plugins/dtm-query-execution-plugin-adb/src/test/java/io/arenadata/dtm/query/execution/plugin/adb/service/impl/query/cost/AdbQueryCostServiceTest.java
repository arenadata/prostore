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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdbQueryCostServiceTest {
    private final QueryEnrichmentService adbQueryEnrichmentService = mock(QueryEnrichmentService.class);
    private final AdbQueryCostService costService = new AdbQueryCostService(adbQueryEnrichmentService);

    @Test
    void calc() {
        initEnrichmentMocks();
        val context = getQueryCostRequestContext();
        TestSuite suite = TestSuite.create("test_suite");
        suite.test("executeQuery", testContext -> {
            Async async = testContext.async();
            costService.calc(context, ar -> {
                if (ar.succeeded()) {
                    testContext.assertEquals(0, ar.result());
                    async.complete();
                } else {
                    testContext.fail(ar.cause());
                }
            });
            async.awaitSuccess(5000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void calcWithEnrichmentError() {
        initEnrichmentBadMocks();
        val context = getQueryCostRequestContext();
        TestSuite suite = TestSuite.create("test_suite");
        suite.test("executeQuery", testContext -> {
            Async async = testContext.async();
            costService.calc(context, ar -> {
                if (ar.succeeded()) {
                    testContext.fail();
                } else {
                    testContext.asyncAssertFailure();
                    async.complete();
                }
            });
            async.awaitSuccess(5000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    private QueryCostRequestContext getQueryCostRequestContext() {
        JsonObject jsonSchema = JsonUtils.init("meta_data.json", "TEST_DATAMART");
        List<Datamart> schema = new ArrayList<>();
        schema.add(jsonSchema.mapTo(Datamart.class));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("SELECT * from PSO");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic("TEST_DATAMART");
        QueryCostRequest costRequest = new QueryCostRequest(queryRequest, schema);
        return new QueryCostRequestContext(new RequestMetrics(), costRequest);
    }

    private void initEnrichmentMocks() {
        AsyncResult<Void> asyncResultEmpty = mock(AsyncResult.class);
        when(asyncResultEmpty.succeeded()).thenReturn(true);

        AsyncResult<List<List<?>>> asyncResult = mock(AsyncResult.class);
        when(asyncResult.succeeded()).thenReturn(true);
        when(asyncResult.result()).thenReturn(new ArrayList<>());
        doAnswer((Answer<AsyncResult<Void>>) arg0 -> {
            ((Handler<AsyncResult<Void>>) arg0.getArgument(1)).handle(asyncResultEmpty);
            return null;
        }).when(adbQueryEnrichmentService).enrich(any(), any());
    }

    private void initEnrichmentBadMocks() {
        doAnswer((Answer<AsyncResult<Void>>) args -> {
            ((Handler<AsyncResult<Void>>) args.getArgument(1)).handle(Future.failedFuture("Enrichment error"));
            return null;
        }).when(adbQueryEnrichmentService).enrich(any(), any());
    }
}
