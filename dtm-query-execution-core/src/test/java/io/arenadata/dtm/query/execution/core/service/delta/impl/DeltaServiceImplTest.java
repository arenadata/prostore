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
package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaService;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.service.metrics.impl.MetricsServiceImpl;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DeltaServiceImplTest {

    private final DeltaQueryParamExtractor deltaQueryParamExtractor = mock(DeltaQueryParamExtractorImpl.class);
    private final DeltaExecutor beginDeltaExecutor = mock(BeginDeltaExecutor.class);
    private final MetricsService<RequestMetrics> metricsService = mock(MetricsServiceImpl.class);
    private DeltaService<QueryResult> deltaService;
    private QueryRequest request = new QueryRequest();

    @BeforeEach
    void setUp() {
        deltaService = new DeltaServiceImpl(deltaQueryParamExtractor,
                Collections.singletonList(beginDeltaExecutor),
                metricsService);
        request.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
    }

    @Test
    void executeWithNullDatamart() {
        Promise promise = Promise.promise();
        DatamartRequest datamartRequest = new DatamartRequest(request);
        DeltaRequestContext context = new DeltaRequestContext(new RequestMetrics(), datamartRequest);

        request.setDatamartMnemonic(null);
        deltaService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithEmptyDatamart() {
        Promise promise = Promise.promise();

        DatamartRequest datamartRequest = new DatamartRequest(request);
        DeltaRequestContext context = new DeltaRequestContext(new RequestMetrics(), datamartRequest);
        request.setDatamartMnemonic("");
        deltaService.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
}
