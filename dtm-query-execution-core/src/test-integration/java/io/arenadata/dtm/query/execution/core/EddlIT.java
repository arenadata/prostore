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

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.execution.core.query.executor.QueryExecutor;
import io.arenadata.dtm.query.execution.core.util.FileUtil;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Promise;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.junit5.VertxExtension;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;

import static io.arenadata.dtm.query.execution.core.util.QueryUtil.*;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@ExtendWith(VertxExtension.class)
public class EddlIT extends AbstractCoreDtmIT {

    @Autowired
    @Qualifier("itTestQueryExecutor")
    private QueryExecutor queryExecutor;

    @Autowired
    @Qualifier("itTestZkExecutor")
    private ZookeeperExecutor zookeeperExecutor;

    @Test
    void uploadExtTableTest() {
        TestSuite suite = TestSuite.create("create upload table tests");
        Promise<ResultSet> promise = Promise.promise();
        final String datamart = "test";
        final String table = "test_ext_table_1";
        final String topic = table;
        suite.test("create table", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, datamart))
                    .compose(v -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/ddl/create_upload_ext_table.sql"),
                                    datamart,
                                    table,
                                    getZkKafkaConnectionString(),
                                    topic)))
                    .compose(v -> zookeeperExecutor.getData(
                            getEntityPath(datamart, table)))
                    .map(result -> {
                        try {
                            final Entity entity = DatabindCodec.mapper().readValue(result, Entity.class);
                            assertSame(entity.getEntityType(), EntityType.UPLOAD_EXTERNAL_TABLE,
                                    "external table create successfully");
                            return result;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_UPLOAD_EXT_TABLE, datamart, table)))
                    .map(resultSet -> {
                        assertNotNull(resultSet, "external table dropped successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_DB, datamart)))
                    .map(resultSet -> {
                        assertNotNull(resultSet, "database dropped successfully");
                        return resultSet;
                    })
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNull(promise.future().cause());
    }

    @Test
    void downloadExtTableTest() {
        TestSuite suite = TestSuite.create("create download ext table tests");
        Promise<?> promise = Promise.promise();
        final String datamart = "test";
        final String table = "test_ext_table_1";
        final String topic = table;
        suite.test("create table", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, datamart))
                    .compose(v -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/ddl/create_download_ext_table.sql"),
                                    datamart,
                                    table,
                                    getZkKafkaConnectionString(),
                                    topic)))
                    .compose(v -> zookeeperExecutor.getData(
                            getEntityPath(datamart, table)))
                    .map(result -> {
                        try {
                            final Entity entity = DatabindCodec.mapper().readValue(result, Entity.class);
                            assertSame(entity.getEntityType(), EntityType.DOWNLOAD_EXTERNAL_TABLE,
                                    "external table create successfully");
                            return result;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_DOWNLOAD_EXT_TABLE,
                            datamart,
                            table)))
                    .map(resultSet -> {
                        assertNotNull(resultSet, "external table dropped successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_DB, datamart)))
                    .map(resultSet -> {
                        assertNotNull(resultSet, "database dropped successfully");
                        return resultSet;
                    })
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNull(promise.future().cause());
    }

}
