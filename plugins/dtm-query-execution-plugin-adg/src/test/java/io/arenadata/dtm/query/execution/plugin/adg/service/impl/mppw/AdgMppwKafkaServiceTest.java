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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl.mppw;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.AdgConnectorApiProperties;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.AdgMppwKafkaProperties;
import io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgHelperTableNamesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgMppwKafkaContextFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.AdgCartridgeError;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.TtLoadDataKafkaResponse;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
@EnabledIfEnvironmentVariable(named = "skipITs", matches = "false")
class AdgMppwKafkaServiceTest {

    private final AdgCartridgeClient client = mock(AdgCartridgeClient.class);
    private final AdgMppwKafkaService service = getAdgMppwKafkaService();

    @BeforeEach
    public void before() {
        Mockito.clearInvocations(client);
    }

    @Test
    void allGoodInitTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context, ar -> {
            assertTrue(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(1)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(1)).transferDataToScdTable(any(), any());
            verify(client, VerificationModeFactory.times(0)).cancelSubscription(any(), any());
        });
    }

    @Test
    void allGoodCancelTest() {
        val context = getRequestContext();
        context.getRequest().setIsLoadStart(false);
        allGoodApiMock();
        service.execute(context, ar -> {
            assertTrue(ar.succeeded());
            verify(client, VerificationModeFactory.times(0)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(0)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
            verify(client, VerificationModeFactory.times(1)).cancelSubscription(any(), any());
        });
    }

    @Test
    void badSubscriptionTest() {
        val context = getRequestContext();
        val service = getAdgMppwKafkaService();
        badSubscribeApiMock1();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(0)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
            verify(client, VerificationModeFactory.times(0)).cancelSubscription(any(), any());
        });
    }

    @Test
    void badSubscriptionTest2() {
        val context = getRequestContext();
        badSubscribeApiMock2();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(0)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
            verify(client, VerificationModeFactory.times(0)).cancelSubscription(any(), any());
        });
    }

    @Test
    void badLoadDataTest() {
        val context = getRequestContext();
        badLoadDataApiMock();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(1)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
        });
    }

    @Test
    void badTransferDataTest() {
        val context = getRequestContext();
        badTransferDataApiMock();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(1)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(1)).transferDataToScdTable(any(), any());
        });
    }

    @Test
    void badCancelTest() {
        val context = getRequestContext();
        context.getRequest().setIsLoadStart(false);
        badCancelApiMock();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(0)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(0)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
        });
    }

    @Test
    void goodAndBadTransferDataTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context, ar -> assertTrue(ar.succeeded()));
        badTransferDataApiMock();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(2)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(2)).transferDataToScdTable(any(), any());
        });
    }

    @Test
    void good2TransferDataTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context, ar -> assertTrue(ar.succeeded()));
        service.execute(context, ar -> {
            assertTrue(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(2)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(2)).transferDataToScdTable(any(), any());
        });
    }

    private AdgMppwKafkaService getAdgMppwKafkaService() {
        val tableNamesFactory = new AdgHelperTableNamesFactoryImpl();
        val connectorApiProperties = new AdgConnectorApiProperties();
        connectorApiProperties.setUrl("https://localhost");
        connectorApiProperties.setKafkaLoadDataUrl("/dataload");
        connectorApiProperties.setKafkaSubscriptionUrl("/sbscription");
        connectorApiProperties.setTransferDataToScdTableUrl("/transferDataToScdTablePath");
        val mppwKafkaProperties = new AdgMppwKafkaProperties();
        mppwKafkaProperties.setMaxNumberOfMessagesPerPartition(200);
        return new AdgMppwKafkaService(
                new AdgMppwKafkaContextFactoryImpl(tableNamesFactory),
                client,
                mppwKafkaProperties
        );
    }

    private MppwRequestContext getRequestContext() {
        val queryRequest = new QueryRequest();
        queryRequest.setEnvName("env1");
        queryRequest.setDatamartMnemonic("test");
        val mppwRequest = new MppwRequest(queryRequest, true, createKafkaParameter());
        return new MppwRequestContext(new RequestMetrics(), mppwRequest);
    }

    private MppwKafkaParameter createKafkaParameter() {
        return MppwKafkaParameter.builder()
                .sysCn(1L)
                .datamart("test")
                .destinationTableName("tbl1")
                .uploadMetadata(UploadExternalEntityMetadata.builder()
                        .name("ext_tab")
                        .externalSchema(getExternalTableSchema())
                        .uploadMessageLimit(1000)
                        .locationPath("kafka://kafka-1.dtm.local:9092/topic")
                        .format(Format.AVRO)
                        .build())
                .brokers(Collections.singletonList(new KafkaBrokerInfo("kafka.host", 9092)))
                .topic("topic1")
                .build();
    }

    private String getExternalTableSchema() {
        return "{\"type\":\"record\",\"name\":\"accounts\",\"namespace\":\"dm2\",\"fields\":[{\"name\":\"column1\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"column2\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"column3\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";
    }

    private void badSubscribeApiMock1() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture("subscribe error"));
            return null;
        }).when(client).subscribe(any(), any());
    }

    private void badSubscribeApiMock2() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new AdgCartridgeError("error", "connector error")));
            return null;
        }).when(client).subscribe(any(), any());
    }

    private void badLoadDataApiMock() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).subscribe(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<TtLoadDataKafkaResponse>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new AdgCartridgeError("error", "connector error")));
            return null;
        }).when(client).loadData(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).cancelSubscription(any(), any());
    }

    private void badTransferDataApiMock() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).subscribe(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<TtLoadDataKafkaResponse>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(new TtLoadDataKafkaResponse(100L)));
            return null;
        }).when(client).loadData(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture("transferDataToScdTable error"));
            return null;
        }).when(client).transferDataToScdTable(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).cancelSubscription(any(), any());
    }

    private void badCancelApiMock() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new AdgCartridgeError("error", "connector error")));
            return null;
        }).when(client).cancelSubscription(any(), any());
    }

    private void allGoodApiMock() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).subscribe(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<TtLoadDataKafkaResponse>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(new TtLoadDataKafkaResponse(100L)));
            return null;
        }).when(client).loadData(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).transferDataToScdTable(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).cancelSubscription(any(), any());
    }

}
