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

import io.arenadata.dtm.query.execution.core.dto.delta.Delta;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.exception.delta.DeltaException;
import io.arenadata.dtm.query.execution.core.query.executor.QueryExecutor;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.util.FileUtil;
import io.vertx.core.Promise;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static io.arenadata.dtm.query.execution.core.util.QueryUtil.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class DeltaIT extends AbstractCoreDtmIT {

    private final String DATAMART = "delta_test";
    private final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final Delta EXPECTED_HOT_DELTA = Delta.builder()
            .hot(HotDelta.builder()
                    .deltaNum(0)
                    .cnFrom(0)
                    .cnMax(-1)
                    .rollingBack(false)
                    .build())
            .build();
    private final Delta EMPTY_DELTA = Delta.builder().build();

    @Autowired
    @Qualifier("itTestQueryExecutor")
    private QueryExecutor queryExecutor;

    @Autowired
    @Qualifier("itTestZkExecutor")
    private ZookeeperExecutor zkExecutor;


    @Test
    void beginDeltaTest() {
        TestSuite suite = TestSuite.create("begin delta tests");
        Promise<?> promise = Promise.promise();
        suite.test("begin delta", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, DATAMART))
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/begin_delta.sql"), DATAMART)))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty());
                        return resultSet;
                    })
                    .compose(resultSet -> zkExecutor.getData(getDeltaPath(DATAMART)))
                    .map(this::deserializeDelta)
                    .map(delta -> {
                        assertEquals(EXPECTED_HOT_DELTA, delta);
                        return delta;
                    })
                    .compose(delta -> queryExecutor.executeQuery(String.format(DROP_DB, DATAMART)))
                    .map(resultSet -> {
                        assertNotNull(resultSet);
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
        assertTrue(promise.future().succeeded());
    }

    @Test
    void beginDeltaWithNumberTest() {
        TestSuite suite = TestSuite.create("begin delta tests");
        Promise<?> promise = Promise.promise();
        final long deltaNumber = 0;
        suite.test("begin delta with number", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, DATAMART))
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/begin_delta_number.sql"), DATAMART, deltaNumber)))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty());
                        return resultSet;
                    })
                    .compose(resultSet -> zkExecutor.getData(getDeltaPath(DATAMART)))
                    .map(this::deserializeDelta)
                    .map(delta -> {
                        assertEquals(EXPECTED_HOT_DELTA, delta);
                        return delta;
                    })
                    .compose(delta -> queryExecutor.executeQuery(String.format(DROP_DB, DATAMART)))
                    .map(resultSet -> {
                        assertNotNull(resultSet);
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
        assertTrue(promise.future().succeeded());
    }

    @Test
    void commitDeltaTest() {
        TestSuite suite = TestSuite.create("commit delta tests");
        Promise<?> promise = Promise.promise();
        final long deltaNumber = 0;
        suite.test("commit delta", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, DATAMART))
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/begin_delta.sql"), DATAMART)))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty());
                        return resultSet;
                    })
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/commit_delta.sql"), DATAMART)))
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(GET_DELTA_BY_NUMBER, deltaNumber)))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty());
                        return resultSet;
                    })
                    .compose(delta -> queryExecutor.executeQuery(String.format(DROP_DB, DATAMART)))
                    .map(resultSet -> {
                        assertNotNull(resultSet);
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
        assertTrue(promise.future().succeeded());
    }

    @Test
    void getDeltaByNumTest() {
        TestSuite suite = TestSuite.create("get delta tests");
        Promise<?> promise = Promise.promise();
        final long deltaNumber = 0;
        suite.test("get delta by number", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, DATAMART))
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/begin_delta.sql"), DATAMART)))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty(), "delta started successfully");
                        return resultSet;
                    })
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/commit_delta.sql"), DATAMART)))
                    .compose(v -> queryExecutor.executeQuery(String.format(GET_DELTA_BY_NUMBER, deltaNumber)))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty(), "delta received by number successfully");
                        return resultSet;
                    })
                    .compose(delta -> queryExecutor.executeQuery(String.format(DROP_DB, DATAMART)))
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
        assertTrue(promise.future().succeeded());
    }

    @Test
    void getDeltaByDatetimeTest() {
        TestSuite suite = TestSuite.create("get delta tests");
        Promise<?> promise = Promise.promise();
        final long deltaNumber = 0;
        suite.test("get delta by datetime", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, DATAMART))
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/begin_delta.sql"), DATAMART)))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty());
                        return resultSet;
                    })
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/commit_delta.sql"), DATAMART)))
                    .compose(v -> queryExecutor.executeQuery(String.format(GET_DELTA_BY_NUMBER, deltaNumber)))
                    .map(this::getDateTimeFromDeltaResult)
                    .compose(localDateTime -> queryExecutor.executeQuery(String.format(GET_DELTA_BY_DATETIME, localDateTime.format(DATETIME_FORMAT))))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty());
                        return resultSet;
                    })
                    .compose(delta -> queryExecutor.executeQuery(String.format(DROP_DB, DATAMART)))
                    .map(resultSet -> {
                        assertNotNull(resultSet);
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
        assertTrue(promise.future().succeeded());
    }

    @Test
    void rollbackDeltaTest() {
        TestSuite suite = TestSuite.create("rollback delta tests");
        Promise<?> promise = Promise.promise();
        suite.test("rollback delta", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, DATAMART))
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/begin_delta.sql"), DATAMART)))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty());
                        return resultSet;
                    })
                    .compose(v -> queryExecutor.executeQuery(String.format(FileUtil.getFileContent("it/queries/delta/rollback_delta.sql"), DATAMART)))
                    .compose(resultSet -> zkExecutor.getData(getDeltaPath(DATAMART)))
                    .map(this::deserializeDelta)
                    .map(delta -> {
                        assertEquals(EMPTY_DELTA, delta);
                        return delta;
                    })
                    .compose(delta -> queryExecutor.executeQuery(String.format(DROP_DB, DATAMART)))
                    .map(resultSet -> {
                        assertNotNull(resultSet);
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
        assertTrue(promise.future().succeeded());
    }

    private Delta deserializeDelta(byte[] bytes) {
        try {
            return DatabindCodec.mapper().readValue(bytes, Delta.class);
        } catch (IOException e) {
            throw new DeltaException("Can't deserialize Delta", e);
        }
    }

    private LocalDateTime getDateTimeFromDeltaResult(ResultSet resultSet) {
        return LocalDateTime.parse(resultSet.getResults().get(0).getString(1), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
    }
}
