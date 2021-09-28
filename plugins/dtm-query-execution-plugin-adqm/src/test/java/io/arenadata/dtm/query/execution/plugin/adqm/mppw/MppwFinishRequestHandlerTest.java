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
package io.arenadata.dtm.query.execution.plugin.adqm.mppw;

import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.query.execution.plugin.adqm.base.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.service.KafkaMppwRequestHandler;
import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.service.MppwFinishRequestHandler;
import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.service.load.RestLoadClient;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockStatusReporter;
import io.arenadata.dtm.query.execution.plugin.adqm.status.dto.StatusReportDto;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MppwFinishRequestHandlerTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());
    private static final String TEST_TOPIC = "adqm_topic";

    @BeforeAll
    public static void setup() {
        ddlProperties.setCluster("test_arenadata");
    }

    @Test
    public void testFinishRequestCallOrder() {
        Map<Predicate<String>, List<Map<String, Object>>> mockData = new HashMap<>();
        mockData.put(t -> t.contains(" from system.columns"),
                Arrays.asList(
                        createRowMap("name", "column1"),
                        createRowMap("name", "column2"),
                        createRowMap("name", "column3"),
                        createRowMap("name", "sys_from"),
                        createRowMap("name", "sys_to"),
                        createRowMap("name", "sys_op"),
                        createRowMap("name", "sys_close_date"),
                        createRowMap("name", "sign")
                ));
        mockData.put(t -> t.contains("select sorting_key from system.tables"),
                Collections.singletonList(
                        createRowMap("sorting_key", "column1, column2")
                ));

        DatabaseExecutor executor = new MockDatabaseExecutor(Arrays.asList(
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("SYSTEM FLUSH DISTRIBUTED dev__shares.accounts_buffer"),
                t -> t.equalsIgnoreCase("SYSTEM FLUSH DISTRIBUTED dev__shares.accounts_actual"),
                t -> t.contains("a.column1, a.column2, a.column3, a.sys_from, 100") && t.contains("dev__shares.accounts_actual") &&
                        t.contains("SEMI LEFT JOIN dev__shares.accounts_buffer_shard b USING(column1, column2)") &&
                        t.contains("sys_from < 101"),
                t -> t.contains("SYSTEM FLUSH DISTRIBUTED dev__shares.accounts_actual"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("OPTIMIZE TABLE dev__shares.accounts_actual_shard ON CLUSTER test_arenadata FINAL")
        ), mockData);

        MockStatusReporter mockReporter = getMockReporter();
        RestLoadClient restLoadClient = mock(RestLoadClient.class);
        when(restLoadClient.stopLoading(any())).thenReturn(Future.succeededFuture());
        KafkaMppwRequestHandler handler = new MppwFinishRequestHandler(restLoadClient, executor,
                ddlProperties,
                appConfiguration,
                mockReporter);

        MppwKafkaRequest request = MppwKafkaRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("shares")
                .envName("env")
                .isLoadStart(true)
                .sysCn(101L)
                .destinationTableName("accounts")
                .topic(TEST_TOPIC)
                .uploadMetadata(new BaseExternalEntityMetadata("", "", ExternalTableFormat.AVRO, ""))
                .build();
        handler.execute(request).onComplete(ar -> {
            assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().getMessage() : "");
            assertTrue(mockReporter.wasCalled("finish"));
        });
    }

    private Map<String, Object> createRowMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private MockStatusReporter getMockReporter() {
        Map<String, StatusReportDto> expected = new HashMap<>();
        expected.put("finish", new StatusReportDto(TEST_TOPIC));
        expected.put("error", new StatusReportDto(TEST_TOPIC));
        return new MockStatusReporter(expected);
    }
}
