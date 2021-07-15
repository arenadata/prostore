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
package io.arenadata.dtm.query.execution.core;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.dto.UnloadSpecDataRequest;
import io.arenadata.dtm.query.execution.core.generator.VendorEmulatorService;
import io.arenadata.dtm.query.execution.core.kafka.StatusMonitorService;
import io.arenadata.dtm.query.execution.core.query.executor.QueryExecutor;
import io.arenadata.dtm.query.execution.core.util.FileUtil;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.UUID;

import static io.arenadata.dtm.query.execution.core.util.QueryUtil.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class EdmlIT extends AbstractCoreDtmIT {

    @Autowired
    @Qualifier("itTestQueryExecutor")
    private QueryExecutor queryExecutor;
    @Autowired
    @Qualifier("itTestWebClient")
    private WebClient webClient;
    @Autowired
    private VendorEmulatorService vendorEmulator;
    @Autowired
    private StatusMonitorService statusMonitorService;
    @Autowired
    @Qualifier("itTestZkKafkaProvider")
    private KafkaZookeeperConnectionProvider zookeeperConnectionProvider;

    @Test
    void vendorEmulatorTest() {
        TestSuite suite = TestSuite.create("vendor emulator tests");
        Promise<?> promise = Promise.promise();
        suite.test("generate data and send into test_topic", context -> {
            Async async = context.async();
            final UnloadSpecDataRequest loadRequest =
                    Json.decodeValue(FileUtil.getFileContent("it/requests/generated_data_check_request.json"),
                            UnloadSpecDataRequest.class);
            vendorEmulator.generateData(getVendorEmulatorHostExternal(), getVendorEmulatorPortExternal(), loadRequest)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertTrue(promise.future().succeeded());
    }

    @Test
    void mppwAdqmTest() {
        TestSuite suite = TestSuite.create("mppw tests");
        Promise<ResultSet> promise = Promise.promise();
        final String datamart = "mppw_test";
        final String destinationTable = "test_table";
        final String sourceTable = "test_table_ext";
        final int rowCount = 1000;
        final String topic = destinationTable + UUID.randomUUID();
        suite.test("mppw", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, datamart))
                    .compose(v -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/mppw/create_destination_table.sql"),
                                    datamart,
                                    destinationTable,
                                    SourceType.ADQM.name())))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet.getResults(), "destination table created successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/mppw/create_source_ext_table.sql"),
                                    datamart,
                                    sourceTable,
                                    getZkKafkaConnectionString(),
                                    topic)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "upload external table created successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> vendorEmulator.generateData(getVendorEmulatorHostExternal(),
                            getVendorEmulatorPortExternal(),
                            Json.decodeValue(String.format(FileUtil.getFileContent("it/requests/generated_data_request.json"),
                                    topic,
                                    rowCount + 1
                            ), UnloadSpecDataRequest.class)))
                    .map(v -> {
                        assertTrue(true, "messages sent to topic successfully");
                        return v;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/delta/begin_delta.sql"),
                                    datamart)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet.getResults().get(0).getString(0),
                                "delta opened successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(INSERT_QUERY,
                            datamart,
                            destinationTable,
                            "*",
                            datamart,
                            sourceTable)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "data inserted successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/delta/commit_delta.sql"),
                                    datamart)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "delta committed successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format("select count(*) as cnt from %s.%s DATASOURCE_TYPE='ADQM'",
                                    datamart,
                                    destinationTable
                            )))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet.getResults().get(0).getLong(0), "inserted rows count");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_DB, datamart)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "database dropped successfully");
                        return resultSet;
                    })
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result());
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess(60000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertTrue(promise.future().succeeded());
        assertNotNull(promise.future().result());
    }

    @Test
    void mpprAdqmTest() {
        TestSuite suite = TestSuite.create("mppr tests");
        Promise<ResultSet> promise = Promise.promise();
        final String datamart = "mppr_test";
        final String dataTable = "test_table";
        final String uploadExtTable = "test_table_upload_ext";
        final String downloadExtTable = "test_table_download_ext";
        final int rowCount = 2000;
        final int msgCount = 2;
        final String uploadTopic = uploadExtTable + UUID.randomUUID();
        final String downloadTopic = downloadExtTable + UUID.randomUUID();
        suite.test("mppr", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, datamart))
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/mppr/create_source_table.sql"),
                                    datamart,
                                    dataTable,
                                    SourceType.ADQM.name())))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "source table created successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/mppw/create_source_ext_table.sql"),
                                    datamart,
                                    uploadExtTable,
                                    getZkKafkaConnectionString(),
                                    uploadTopic)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "upload external table created successfully");
                        return resultSet;
                    })
                    .compose(v -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/mppr/create_destination_ext_table.sql"),
                                    datamart,
                                    downloadExtTable,
                                    getZkKafkaConnectionString(),
                                    downloadTopic)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet.getResults(), "download external table created successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> vendorEmulator.generateData(getVendorEmulatorHostExternal(),
                            getVendorEmulatorPortExternal(),
                            Json.decodeValue(String.format(FileUtil.getFileContent("it/requests/generated_data_request.json"),
                                    uploadTopic,
                                    rowCount + 1
                            ), UnloadSpecDataRequest.class)))
                    .map(v -> {
                        assertTrue(true, "messages sent to topic successfully");
                        return v;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/delta/begin_delta.sql"),
                                    datamart)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet.getResults().get(0).getString(0),
                                "delta opened successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(INSERT_QUERY,
                            datamart,
                            dataTable,
                            "*",
                            datamart,
                            uploadExtTable)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "data inserted successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/delta/commit_delta.sql"),
                                    datamart)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "delta committed successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(INSERT_QUERY,
                            datamart,
                            downloadExtTable,
                            "*",
                            datamart,
                            dataTable)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "data exload successfully");
                        return resultSet;
                    })
                    .compose(resultSet ->
                            statusMonitorService.getTopicStatus(getKafkaStatusMonitorHostExternal(),
                                    getKafkaStatusMonitorPortExternal(),
                                    new StatusRequest(downloadTopic, getAdqmConsumerGroup())))
                    .map(statusResponse -> {
                        assertTrue(statusResponse.getProducerOffset() >= msgCount,
                                "Messages loaded into topic successfully");
                        return statusResponse;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_DB, datamart)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "database dropped successfully");
                        return resultSet;
                    })
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result());
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess(60000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertTrue(promise.future().succeeded());
        assertNotNull(promise.future().result());
    }
}