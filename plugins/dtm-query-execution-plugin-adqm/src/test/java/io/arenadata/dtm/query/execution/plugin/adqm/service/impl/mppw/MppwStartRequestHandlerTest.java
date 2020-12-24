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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.StatusReportDto;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmRestMppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.impl.AdqmRestMppwKafkaRequestFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.LoadType;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.RestLoadClient;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.RestMppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockStatusReporter;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.vertx.core.Future;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.function.Predicate;

import static io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.LoadType.KAFKA;
import static io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.LoadType.REST;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MppwStartRequestHandlerTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());

    private static final String TEST_TOPIC = "adqm_topic";
    private static final String TEST_CONSUMER_GROUP = "adqm_group";
    private final AdqmRestMppwKafkaRequestFactory mppwKafkaRequestFactory = mock(AdqmRestMppwKafkaRequestFactoryImpl.class);
    private final List<KafkaBrokerInfo> kafkaBrokers = Collections.singletonList(new KafkaBrokerInfo("kafka.host", 9092));

    @BeforeAll
    public static void setup() {
        ddlProperties.setTtlSec(3600);
        ddlProperties.setCluster("test_arenadata");
        ddlProperties.setArchiveDisk("default");
    }

    @Test
    public void testStartCallOrder() {
        Map<Predicate<String>, List<Map<String, Object>>> mockData = new HashMap<>();
        mockData.put(t -> t.contains("select engine_full"), Collections.singletonList(
                createRowMap("engine_full", "Distributed('test_arenadata', 'shares', 'accounts_actual_shard', column1)")
        ));
        mockData.put(t -> t.contains("select sorting_key"), Collections.singletonList(
                createRowMap("sorting_key", "column1, column2, sys_from")
        ));

        DatabaseExecutor executor = new MockDatabaseExecutor(Arrays.asList(
                t -> t.contains("CREATE TABLE IF NOT EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata") &&
                        t.contains("column1 Nullable(Int64), column2 Nullable(Int64), column3 Nullable(String), sys_op Nullable(Int64)") &&
                        t.contains("ENGINE = Kafka()"),
                t -> t.equalsIgnoreCase("CREATE TABLE IF NOT EXISTS dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata (column1 Int64, column2 Int64, sys_op_buffer Nullable(Int8)) ENGINE = Join(ANY, INNER, column1, column2)"),
                t -> t.equalsIgnoreCase("CREATE TABLE IF NOT EXISTS dev__shares.accounts_buffer ON CLUSTER test_arenadata AS dev__shares.accounts_buffer_shard ENGINE=Distributed('test_arenadata', 'shares', 'accounts_buffer_shard', column1)"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_buffer\n" +
                        "  AS SELECT column1, column2, sys_op AS sys_op_buffer FROM dev__shares.accounts_ext_shard"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_actual\n" +
                        "AS SELECT es.column1, es.column2, es.column3, 101 AS sys_from, 9223372036854775807 as sys_to, 0 as sys_op_load, '9999-12-31 00:00:00' as close_date, 1 AS sign  FROM dev__shares.accounts_ext_shard es WHERE es.sys_op <> 1")
        ), mockData, false);

        MockStatusReporter mockReporter = createMockReporter(TEST_CONSUMER_GROUP + "dev__shares.accounts");
        RestLoadClient mockInitiator = Mockito.mock(RestLoadClient.class);
        MppwRequestHandler handler = new MppwStartRequestHandler(executor, ddlProperties, appConfiguration,
                createMppwProperties(KAFKA),
                mockReporter, mockInitiator, mppwKafkaRequestFactory);

        MppwRequest request = new MppwRequest(QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("shares").build(),
                true, MppwKafkaParameter.builder()
                .datamart("shares")
                .sysCn(101L)
                .destinationTableName("accounts")
                .uploadMetadata(UploadExternalEntityMetadata.builder()
                        .externalSchema(getSchema())
                        .format(Format.AVRO)
                        .uploadMessageLimit(1000)
                        .build())
                .topic(TEST_TOPIC)
                .brokers(kafkaBrokers)
                .build());

        RestMppwKafkaLoadRequest restRequest = RestMppwKafkaLoadRequest.builder()
                .requestId(request.getQueryRequest().getRequestId().toString())
                .datamart(request.getKafkaParameter().getDatamart())
                .tableName(request.getKafkaParameter().getDestinationTableName())
                .kafkaTopic(request.getKafkaParameter().getTopic())
                .kafkaBrokers(request.getKafkaParameter().getBrokers())
                .hotDelta(request.getKafkaParameter().getSysCn())
                .consumerGroup("mppwProperties.getRestLoadConsumerGroup()")
                .format(request.getKafkaParameter().getUploadMetadata().getFormat().getName())
                .schema(new Schema.Parser().parse(request.getKafkaParameter().getUploadMetadata().getExternalSchema()))
                .messageProcessingLimit(((UploadExternalEntityMetadata)request.getKafkaParameter().getUploadMetadata())
                        .getUploadMessageLimit() == null ? 0 :
                        ((UploadExternalEntityMetadata)request.getKafkaParameter().getUploadMetadata()).getUploadMessageLimit())
                .build();

        when(mppwKafkaRequestFactory.create(request)).thenReturn(restRequest);

        handler.execute(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().getMessage() : "");
                    assertTrue(mockReporter.wasCalled("start"));
                    verify(mockInitiator, never()).initiateLoading(any());
                });
    }

    @Test
    public void testStartCallOrderWithRest() {
        Map<Predicate<String>, List<Map<String, Object>>> mockData = new HashMap<>();
        mockData.put(t -> t.contains("select engine_full"),
                Collections.singletonList(
                        createRowMap("engine_full", "Distributed('test_arenadata', 'shares', 'accounts_actual_shard', column1)")
                ));
        mockData.put(t -> t.contains("select sorting_key"),
                Collections.singletonList(
                        createRowMap("sorting_key", "column1, column2, sys_from")
                ));

        DatabaseExecutor executor = new MockDatabaseExecutor(Arrays.asList(
                t -> t.contains("CREATE TABLE IF NOT EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata") &&
                        t.contains("column1 Int64, column2 Int64, column3 Nullable(String), sys_op Nullable(Int64)") &&
                        t.contains("ENGINE = MergeTree()") &&
                        t.contains("ORDER BY (column1, column2)"),
                t -> t.equalsIgnoreCase("CREATE TABLE IF NOT EXISTS dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata (column1 Int64, column2 Int64, sys_op_buffer Nullable(Int8)) ENGINE = Join(ANY, INNER, column1, column2)"),
                t -> t.equalsIgnoreCase("CREATE TABLE IF NOT EXISTS dev__shares.accounts_buffer ON CLUSTER test_arenadata AS dev__shares.accounts_buffer_shard ENGINE=Distributed('test_arenadata', 'shares', 'accounts_buffer_shard', column1)"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_buffer\n" +
                        "  AS SELECT column1, column2, sys_op AS sys_op_buffer FROM dev__shares.accounts_ext_shard"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_actual\n" +
                        "AS SELECT es.column1, es.column2, es.column3, 101 AS sys_from, 9223372036854775807 as sys_to, 0 as sys_op_load, '9999-12-31 00:00:00' as close_date, 1 AS sign  FROM dev__shares.accounts_ext_shard es WHERE es.sys_op <> 1")
        ), mockData, false);

        MockStatusReporter mockReporter = createMockReporter("restConsumerGroup");
        RestLoadClient mockInitiator = Mockito.mock(RestLoadClient.class);
        when(mockInitiator.initiateLoading(any())).thenReturn(Future.succeededFuture());

        MppwRequestHandler handler = new MppwStartRequestHandler(executor, ddlProperties, appConfiguration,
                createMppwProperties(REST),
                mockReporter, mockInitiator, mppwKafkaRequestFactory);
        MppwRequest request = new MppwRequest(QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .sourceType(SourceType.ADQM)
                .datamartMnemonic("shares").build(),
                true, MppwKafkaParameter.builder()
                .datamart("shares")
                .sysCn(101L)
                .destinationTableName("accounts")
                .uploadMetadata(UploadExternalEntityMetadata.builder()
                        .externalSchema(getSchema())
                        .format(Format.AVRO)
                        .uploadMessageLimit(1000)
                        .build())
                .topic(TEST_TOPIC)
                .brokers(kafkaBrokers)
                .build());

        RestMppwKafkaLoadRequest restRequest = RestMppwKafkaLoadRequest.builder()
                .requestId(request.getQueryRequest().getRequestId().toString())
                .datamart(request.getKafkaParameter().getDatamart())
                .tableName(request.getKafkaParameter().getDestinationTableName())
                .kafkaTopic(request.getKafkaParameter().getTopic())
                .kafkaBrokers(request.getKafkaParameter().getBrokers())
                .hotDelta(request.getKafkaParameter().getSysCn())
                .consumerGroup("mppwProperties.getRestLoadConsumerGroup()")
                .format(request.getKafkaParameter().getUploadMetadata().getFormat().getName())
                .schema(new Schema.Parser().parse(request.getKafkaParameter().getUploadMetadata().getExternalSchema()))
                .messageProcessingLimit(((UploadExternalEntityMetadata)request.getKafkaParameter().getUploadMetadata())
                        .getUploadMessageLimit() == null ? 0 :
                        ((UploadExternalEntityMetadata)request.getKafkaParameter().getUploadMetadata()).getUploadMessageLimit())
                .build();

        when(mppwKafkaRequestFactory.create(request)).thenReturn(restRequest);

        handler.execute(request).onComplete(ar -> {
            assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().getMessage() : "");
            assertTrue(mockReporter.wasCalled("start"));
            verify(mockInitiator, only()).initiateLoading(any());
        });
    }

    private Map<String, Object> createRowMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private MockStatusReporter createMockReporter(String expectedConsumerGroup) {
        Map<String, StatusReportDto> expected = new HashMap<>();
        expected.put("start", new StatusReportDto(TEST_TOPIC, expectedConsumerGroup));
        return new MockStatusReporter(expected);
    }

    private String getSchema() {
        return "{\"type\":\"record\",\"name\":\"accounts\",\"namespace\":\"dm2\",\"fields\":[{\"name\":\"column1\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"column2\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"column3\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";
    }

    private MppwProperties createMppwProperties(LoadType loadType) {
        MppwProperties mppwProperties = new MppwProperties();
        mppwProperties.setConsumerGroup(TEST_CONSUMER_GROUP);
        mppwProperties.setKafkaBrokers("localhost:9092");
        mppwProperties.setLoadType(loadType);
        mppwProperties.setRestLoadConsumerGroup("restConsumerGroup");
        return mppwProperties;
    }
}
