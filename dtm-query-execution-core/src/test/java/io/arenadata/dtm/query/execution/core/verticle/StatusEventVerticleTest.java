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
package io.arenadata.dtm.query.execution.core.verticle;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.PublishStatusEventRequest;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.delta.OpenDeltaEvent;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaStatusEventPublisher;
import io.arenadata.dtm.query.execution.core.CoreTestConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.impl.BeginDeltaExecutor;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.dto.delta.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.producer.KafkaProducer;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ActiveProfiles("test")
@SpringBootTest(classes = {CoreTestConfiguration.class, StatusEventVerticle.class})
@ExtendWith(VertxExtension.class)
class StatusEventVerticleTest {
    public static final long EXPECTED_SIN_ID = 2L;
    public static final String EXPECTED_DATAMART = "test_datamart";
    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    @MockBean
    KafkaAdminClient kafkaAdminClient;
    @MockBean
    KafkaConsumerMonitor kafkaConsumerMonitor;
    @MockBean
    KafkaProducer<String, String> jsonCoreKafkaProducer;
    @MockBean
    ServiceDbFacade serviceDbFacade;
    @MockBean
    DeltaServiceDao deltaServiceDao;
    @MockBean
    @Qualifier("beginDeltaQueryResultFactory")
    DeltaQueryResultFactory deltaQueryResultFactory;
    @MockBean
    KafkaStatusEventPublisher kafkaStatusEventPublisher;
    @Autowired
    private BeginDeltaExecutor beginDeltaExecutor;

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic(EXPECTED_DATAMART);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setDatamart(req.getDatamartMnemonic());
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
    }

    @Test
    void publishDeltaOpenEvent(VertxTestContext testContext) throws InterruptedException {
        req.setSql("BEGIN DELTA");
        long deltaNum = 1L;
        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart("test")
                .request(req)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum));

        when(deltaServiceDao.writeNewDeltaHot(any(), any())).thenReturn(Future.succeededFuture(1L));
        when(deltaQueryResultFactory.create(any()))
                .thenReturn(queryResult);

        Mockito.doAnswer(invocation -> {
            try {
                final PublishStatusEventRequest<OpenDeltaEvent> request = invocation.getArgument(0);
                final Handler<AsyncResult<?>> handler = invocation.getArgument(1);
                handler.handle(Future.succeededFuture());
                assertNotNull(request);
                assertNotNull(request.getEventKey());
                assertNotNull(request.getEventMessage());
                assertEquals(StatusEventCode.DELTA_OPEN, request.getEventKey().getEvent());
                assertEquals(EXPECTED_DATAMART, request.getEventKey().getDatamart());
                assertEquals(EXPECTED_SIN_ID, request.getEventMessage().getDeltaNum());
                testContext.completeNow();
            } catch (Exception ex) {
                testContext.failNow(ex);
            }
            return null;
        }).when(kafkaStatusEventPublisher).publish(any(), any());
        beginDeltaExecutor.execute(deltaQuery, handler -> {
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    private List<Map<String, Object>> createResult(Long deltaNum) {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.NUM_FIELD),
                Collections.singletonList(deltaNum));
    }

}
