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

import io.arenadata.dtm.query.execution.core.query.executor.QueryExecutor;
import io.vertx.core.Promise;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.junit5.VertxExtension;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@ExtendWith(VertxExtension.class)
public class DmlIT extends AbstractCoreDtmIT {

    private ResultSet resultSet;
    @Autowired
    @Qualifier("itTestQueryExecutor")
    private QueryExecutor queryExecutor;

    @SneakyThrows
    @Test
    void InformationSchemaTest() throws IOException {
        TestSuite suite = TestSuite.create("information schema tests");
        Promise<?> promise = Promise.promise();
        suite.test("select tables", testContext1 -> {
            Async async = testContext1.async();
            queryExecutor.executeQuery("select * from information_schema.tables")
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
    @Disabled
    void llrAdqmTest() {
        TestSuite suite = TestSuite.create("select from adqm");
        Promise<?> promise = Promise.promise();
        suite.test("select with datasource type", testContext1 -> {
            Async async = testContext1.async();
            queryExecutor.executeQuery("select * from transactions DATASOURCE_TYPE='ADQM'")
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
