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
package io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.service;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.response.AdgCartridgeError;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.response.TtLoadDataKafkaResponse;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.mppw.configuration.properties.AdgMppwKafkaProperties;
import io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.factory.AdgMppwKafkaContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
        service.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).cancelSubscription(any());
                });
    }

    @Test
    void testMaxNumberOfMessagesFromEntity() {
        val context = getRequestContext();
        long maxNumberOfMessages = 300L;
        context.getSourceEntity().setExternalTableUploadMessageLimit((int) maxNumberOfMessages);
        allGoodApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(
                            argThat(request -> maxNumberOfMessages == request.getMaxNumberOfMessagesPerPartition()));
                });
    }

    @Test
    void testMaxNumberOfMessagesFromProperties() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(
                            argThat(request -> 200L == request.getMaxNumberOfMessagesPerPartition()));
                });
    }

    @Test
    void allGoodCancelTest() {
        val context = getRequestContext();
        context.setIsLoadStart(false);
        allGoodApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(client, VerificationModeFactory.times(0)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).loadData(any());
                    verify(client, VerificationModeFactory.times(1)).cancelSubscription(any());
                });
    }

    @Test
    void badSubscriptionTest() {
        val context = getRequestContext();
        val service = getAdgMppwKafkaService();
        badSubscribeApiMock1();
        service.execute(context)
                .onComplete(ar -> {
                    assertFalse(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).loadData(any());
                    verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any());
                    verify(client, VerificationModeFactory.times(0)).cancelSubscription(any());
                });
    }

    @Test
    void badSubscriptionTest2() {
        val context = getRequestContext();
        badSubscribeApiMock2();
        service.execute(context)
                .onComplete(ar -> {
                    assertFalse(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).loadData(any());
                    verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any());
                    verify(client, VerificationModeFactory.times(0)).cancelSubscription(any());
                });
    }

    @Test
    void badLoadDataTest() {
        val context = getRequestContext();
        badLoadDataApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertEquals(ar.result(), QueryResult.emptyResult());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any());
                });
    }

    @Test
    void badTransferDataTest() {
        val context = getRequestContext();
        badTransferDataApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertEquals(ar.result(), QueryResult.emptyResult());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                });
    }

    @Test
    void badCancelTest() {
        val context = getRequestContext();
        context.setIsLoadStart(false);
        badCancelApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertFalse(ar.succeeded());
                    verify(client, VerificationModeFactory.times(0)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).loadData(any());
                });
    }

    @Test
    void goodAndBadTransferDataTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        badTransferDataApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertFalse(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(1)).transferDataToScdTable(any());
                });
    }

    @Test
    void good2TransferDataTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        service.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(1)).transferDataToScdTable(any());
                });
    }

    private AdgMppwKafkaService getAdgMppwKafkaService() {
        val tableNamesFactory = new AdgHelperTableNamesFactory();
        val mppwKafkaProperties = new AdgMppwKafkaProperties();
        mppwKafkaProperties.setMaxNumberOfMessagesPerPartition(200);
        return new AdgMppwKafkaService(
                new AdgMppwKafkaContextFactory(tableNamesFactory),
                client,
                mppwKafkaProperties
        );
    }

    private MppwRequest getRequestContext() {
        return MppwKafkaRequest.builder()
                .envName("env1")
                .datamartMnemonic("test")
                .isLoadStart(true)
                .sysCn(1L)
                .sourceEntity(Entity.builder()
                        .build())
                .destinationEntity(Entity.builder().name("tbl1").build())
                .uploadMetadata(UploadExternalEntityMetadata.builder()
                        .name("ext_tab")
                        .externalSchema(getExternalTableSchema())
                        .uploadMessageLimit(1000)
                        .locationPath("kafka://kafka-1.dtm.local:9092/topic")
                        .format(ExternalTableFormat.AVRO)
                        .build())
                .brokers(Collections.singletonList(new KafkaBrokerInfo("kafka.host", 9092)))
                .topic("topic1")
                .build();
    }

    private String getExternalTableSchema() {
        return "{\"type\":\"record\",\"name\":\"accounts\",\"namespace\":\"dm2\",\"fields\":[{\"name\":\"column1\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"column2\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"column3\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";
    }

    private void badSubscribeApiMock1() {
        when(client.subscribe(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("subscribe error")));
    }

    private void badSubscribeApiMock2() {
        when(client.subscribe(any()))
                .thenReturn(Future.failedFuture(new AdgCartridgeError("error", "connector error")));
    }

    private void badLoadDataApiMock() {
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());
        when(client.loadData(any()))
                .thenReturn(Future.failedFuture(new AdgCartridgeError("error", "connector error")));
        when(client.cancelSubscription(any())).thenReturn(Future.succeededFuture());
    }

    private void badTransferDataApiMock() {
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());
        when(client.loadData(any()))
                .thenReturn(Future.succeededFuture(new TtLoadDataKafkaResponse(100L)));
        when(client.transferDataToScdTable(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("transferDataToScdTable error")));
        when(client.cancelSubscription(any())).thenReturn(Future.succeededFuture());
    }

    private void badCancelApiMock() {
        when(client.cancelSubscription(any()))
                .thenReturn(Future.failedFuture(new AdgCartridgeError("error", "connector error")));
    }

    private void allGoodApiMock() {
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());
        when(client.loadData(any())).thenReturn(Future.succeededFuture(new TtLoadDataKafkaResponse(100L)));
        when(client.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());
        when(client.cancelSubscription(any())).thenReturn(Future.succeededFuture());
    }

}
