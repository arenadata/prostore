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
package io.arenadata.dtm.query.execution.core.edml;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.configuration.kafka.KafkaAdminProperty;
import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesServiceImpl;
import io.arenadata.dtm.query.execution.core.edml.configuration.EdmlProperties;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopReason;
import io.arenadata.dtm.query.execution.core.edml.mppw.factory.MppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.edml.mppw.factory.impl.MppwErrorMessageFactoryImpl;
import io.arenadata.dtm.query.execution.core.edml.mppw.factory.impl.MppwKafkaRequestFactoryImpl;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.EdmlUploadExecutor;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.BreakMppwContext;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.UploadKafkaExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UploadKafkaExecutorTest {
    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private final MppwKafkaRequestFactory mppwKafkaRequestFactory = mock(MppwKafkaRequestFactoryImpl.class);
    private final EdmlProperties edmlProperties = mock(EdmlProperties.class);
    private final KafkaProperties kafkaProperties = mock(KafkaProperties.class);
    private final CheckColumnTypesService checkColumnTypesService = mock(CheckColumnTypesServiceImpl.class);
    private final DtmConfig dtmSettings = mock(CoreDtmSettings.class);
    private final Vertx vertx = Vertx.vertx();
    private final Integer inputStreamTimeoutMs = 2000;
    private final Integer pluginStatusCheckPeriodMs = 1000;
    private final Integer firstOffsetTimeoutMs = 15000;
    private final Integer changeOffsetTimeoutMs = 10000;
    private final long msgCommitTimeoutMs = 1000L;
    private final long msgProcessTimeoutMs = 100L;
    private EdmlUploadExecutor uploadKafkaExecutor;
    private Set<SourceType> sourceTypes;
    private QueryRequest queryRequest;
    private QueryResult queryResult;
    private Throwable resultException;
    private final MppwKafkaRequest pluginRequest = MppwKafkaRequest.builder()
            .requestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"))
            .envName("env")
            .datamartMnemonic("test")
            .sysCn(1L)
            .isLoadStart(true)
            .build();

    private ZoneId timeZone;

    @BeforeEach
    void setUp() {
        uploadKafkaExecutor = new UploadKafkaExecutor(pluginService,
                mppwKafkaRequestFactory,
                edmlProperties,
                kafkaProperties,
                vertx,
                dtmSettings,
                new MppwErrorMessageFactoryImpl(),
                checkColumnTypesService);
        sourceTypes = new HashSet<>();
        sourceTypes.addAll(Arrays.asList(SourceType.ADB, SourceType.ADG));
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSql("INSERT INTO test.pso SELECT id, name FROM test.upload_table");
        when(dtmSettings.getTimeZone()).thenReturn(ZoneId.of("UTC"));
        timeZone = dtmSettings.getTimeZone();
        when(checkColumnTypesService.check(any(), any())).thenReturn(Future.succeededFuture(true));
    }

    @AfterEach
    public void cleanUp() {
        queryResult = null;
        resultException = null;
        BreakMppwContext.removeTask(pluginRequest.getDatamartMnemonic(), pluginRequest.getSysCn());
    }

    @Test
    void executeMppwAllSuccess() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwAllSuccess", context -> {
            Async async = context.async();
            Promise<QueryResult> promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inputStreamTimeoutMs);

            EdmlRequestContext edmlRequestContext = createEdmlRequestContext();

            final Queue<MppwKafkaRequest> mppwContextQueue = new BlockingArrayQueue<>();
            mppwContextQueue.add(pluginRequest);
            mppwContextQueue.add(pluginRequest);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adbStatusResultQueue, 15, 5);
            initStatusResultQueue(adgStatusResultQueue, 15, 5);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);
            when(mppwKafkaRequestFactory.create(edmlRequestContext))
                    .thenReturn(Future.succeededFuture(mppwContextQueue.poll()));
            when(pluginService.mppw(eq(SourceType.ADB), any(), eq(pluginRequest)))
                    .thenReturn(Future.succeededFuture());
            when(pluginService.mppw(eq(SourceType.ADG), any(), eq(pluginRequest)))
                    .thenReturn(Future.succeededFuture());

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(adbStatusResultQueue.poll());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(adgStatusResultQueue.poll());
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(new QueryResult());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(new QueryResult());
                }
                return null;
            }).when(pluginService).mppw(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result());
                            async.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
            async.awaitSuccess();
            queryResult = promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(queryResult);
        assertThat(BreakMppwContext.getNumberOfTasksByDatamart(pluginRequest.getDatamartMnemonic())).isEqualTo(0);
    }

    @Test
    void testBreakMppwTaskStopsExecution() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwAllSuccess", context -> {
            Async async = context.async();
            Promise<QueryResult> promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inputStreamTimeoutMs);

            EdmlRequestContext edmlRequestContext = createEdmlRequestContext();

            final Queue<MppwKafkaRequest> mppwContextQueue = new BlockingArrayQueue<>();
            mppwContextQueue.add(pluginRequest);
            mppwContextQueue.add(pluginRequest);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adbStatusResultQueue, 15, 5);
            initStatusResultQueue(adgStatusResultQueue, 15, 5);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);
            when(mppwKafkaRequestFactory.create(edmlRequestContext))
                    .thenReturn(Future.succeededFuture(mppwContextQueue.poll()));
            when(pluginService.mppw(eq(SourceType.ADB), any(), eq(pluginRequest)))
                    .thenReturn(Future.succeededFuture());
            when(pluginService.mppw(eq(SourceType.ADG), any(), eq(pluginRequest)))
                    .thenReturn(Future.succeededFuture());

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(adbStatusResultQueue.poll());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(adgStatusResultQueue.poll());
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(new QueryResult());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(new QueryResult());
                }
                return null;
            }).when(pluginService).mppw(any(), any(), any());

            BreakMppwContext.requestRollback(pluginRequest.getDatamartMnemonic(),
                    pluginRequest.getSysCn(),
                    MppwStopReason.BREAK_MPPW_RECEIVED);
            uploadKafkaExecutor.execute(edmlRequestContext)
                    .onComplete(ar -> {
                        if (ar.failed()) {
                            resultException = ar.cause();
                            promise.complete();
                            async.complete();
                        } else {
                            promise.fail("Should have been stopped mppw");
                        }
                    });
            async.awaitSuccess();
            queryResult = promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));

        assertNotNull(resultException);
        assertThat(resultException.getMessage()).contains(MppwStopReason.BREAK_MPPW_RECEIVED.toString());
    }

    @Test
    void executeMppwWithAdbPluginStartFail() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwWithStartFail", context -> {
            Async async = context.async();
            Promise promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inputStreamTimeoutMs);

            EdmlRequestContext edmlRequestContext = createEdmlRequestContext();

            final Queue<MppwKafkaRequest> mppwContextQueue = new BlockingArrayQueue<>();
            mppwContextQueue.add(pluginRequest);
            mppwContextQueue.add(pluginRequest);

            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adgStatusResultQueue, 10, 5);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext))
                    .thenReturn(Future.succeededFuture(mppwContextQueue.poll()));
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(adgStatusResultQueue.poll());
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                final MppwRequest requestContext = invocation.getArgument(2);
                if (ds.equals(SourceType.ADB) && requestContext.getIsLoadStart()) {
                    return Future.failedFuture(new DtmException("Start mppw error"));
                } else if (ds.equals(SourceType.ADB) && !requestContext.getIsLoadStart()) {
                    return Future.succeededFuture(new QueryResult());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(new QueryResult());
                }
                return null;
            }).when(pluginService).mppw(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result());
                        } else {
                            resultException = ar.cause();
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess();
            queryResult = (QueryResult) promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));

        assertThat(BreakMppwContext.getReason(
                pluginRequest.getDatamartMnemonic(),
                pluginRequest.getSysCn()))
                .isEqualTo(MppwStopReason.UNABLE_TO_START);
        assertThat(BreakMppwContext.getNumberOfTasksByDatamart(pluginRequest.getDatamartMnemonic())).isEqualTo(1);
        assertNotNull(resultException);
    }

    @Test
    void executeMppwWithFailedRetrievePluginStatus() {
        RuntimeException exception = new DtmException("Error getting plugin status: ADB");
        KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
        kafkaAdminProperty.setInputStreamTimeoutMs(inputStreamTimeoutMs);

        EdmlRequestContext edmlRequestContext = createEdmlRequestContext();

        final Queue<MppwKafkaRequest> mppwContextQueue = new BlockingArrayQueue<>();
        mppwContextQueue.add(pluginRequest);
        mppwContextQueue.add(pluginRequest);

        final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
        final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
        initStatusResultQueue(adbStatusResultQueue, 10, 5);
        initStatusResultQueue(adgStatusResultQueue, 10, 5);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
        when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
        when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
        when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
        when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

        when(mppwKafkaRequestFactory.create(edmlRequestContext))
                .thenReturn(Future.succeededFuture(mppwContextQueue.poll()));

        Mockito.doAnswer(invocation -> {
            final SourceType ds = invocation.getArgument(0);
            if (ds.equals(SourceType.ADB)) {
                return Future.failedFuture(exception);
            } else if (ds.equals(SourceType.ADG)) {
                return Future.failedFuture(exception);
            }
            return null;
        }).when(pluginService).status(any(), any(), any());

        Mockito.doAnswer(invocation -> {
            final SourceType ds = invocation.getArgument(0);
            if (ds.equals(SourceType.ADB)) {
                return Future.succeededFuture(new QueryResult());
            } else if (ds.equals(SourceType.ADG)) {
                return Future.succeededFuture(new QueryResult());
            }
            return null;
        }).when(pluginService).mppw(any(), any(), any());

        uploadKafkaExecutor.execute(edmlRequestContext)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals(exception.getMessage(), ar.cause().getMessage());
                });
    }

    @Test
    void executeMppwWithLastOffsetNotIncrease() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwWithLastOffsetNotIncrease", context -> {
            Async async = context.async();
            Promise promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inputStreamTimeoutMs);

            EdmlRequestContext edmlRequestContext = createEdmlRequestContext();

            final Queue<MppwKafkaRequest> mppwContextQueue = new BlockingArrayQueue<>();
            mppwContextQueue.add(pluginRequest);
            mppwContextQueue.add(pluginRequest);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adbStatusResultQueue, 15, 5);
            initStatusResultQueueWithOffset(adgStatusResultQueue, 15, 5, 1);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext))
                    .thenReturn(Future.succeededFuture(mppwContextQueue.poll()));

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(adbStatusResultQueue.poll());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(adgStatusResultQueue.poll());
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(new QueryResult());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(new QueryResult());
                }
                return null;
            }).when(pluginService).mppw(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result());
                        } else {
                            resultException = ar.cause();
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess();
            queryResult = (QueryResult) promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(resultException);

        assertThat(BreakMppwContext.getReason(
                pluginRequest.getDatamartMnemonic(),
                pluginRequest.getSysCn()))
                .isEqualTo(MppwStopReason.CHANGE_OFFSET_TIMEOUT);
        assertThat(BreakMppwContext.getNumberOfTasksByDatamart(pluginRequest.getDatamartMnemonic())).isEqualTo(1);
    }

    @Test
    void executeMppwLoadingInitFalure() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwLoadingInitFailure", context -> {
            Async async = context.async();
            Promise<QueryResult> promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inputStreamTimeoutMs);

            EdmlRequestContext edmlRequestContext = createEdmlRequestContext();

            final Queue<MppwKafkaRequest> mppwContextQueue = new BlockingArrayQueue<>();
            mppwContextQueue.add(pluginRequest);
            mppwContextQueue.add(pluginRequest);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adbStatusResultQueue, 15, 5);
            initStatusResultQueueWithOffset(adgStatusResultQueue, 15, 5, 0);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext))
                    .thenReturn(Future.succeededFuture(mppwContextQueue.poll()));

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(adbStatusResultQueue.poll());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(adgStatusResultQueue.poll());
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(new QueryResult());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(new QueryResult());
                }
                return null;
            }).when(pluginService).mppw(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result());
                        } else {
                            resultException = ar.cause();
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess();
            queryResult = promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(resultException);

        assertThat(BreakMppwContext.getReason(
                pluginRequest.getDatamartMnemonic(),
                pluginRequest.getSysCn()))
                .isEqualTo(MppwStopReason.FIRST_OFFSET_TIMEOUT);
        assertThat(BreakMppwContext.getNumberOfTasksByDatamart(pluginRequest.getDatamartMnemonic())).isEqualTo(1);
    }

    @Test
    void executeMppwWithZeroOffsets() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwWithZeroOffsets", context -> {
            Async async = context.async();
            Promise<QueryResult> promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inputStreamTimeoutMs);

            EdmlRequestContext edmlRequestContext = createEdmlRequestContext();

            final Queue<MppwKafkaRequest> mppwContextQueue = new BlockingArrayQueue<>();
            mppwContextQueue.add(pluginRequest);
            mppwContextQueue.add(pluginRequest);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueueWithOffset(adbStatusResultQueue, 15, 0, 0);
            initStatusResultQueueWithOffset(adgStatusResultQueue, 15, 0, 0);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext))
                    .thenReturn(Future.succeededFuture(mppwContextQueue.poll()));

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(adbStatusResultQueue.poll());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(adgStatusResultQueue.poll());
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    return Future.succeededFuture(new QueryResult());
                } else if (ds.equals(SourceType.ADG)) {
                    return Future.succeededFuture(new QueryResult());
                }
                return null;
            }).when(pluginService).mppw(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result());
                        } else {
                            resultException = ar.cause();
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess();
            queryResult = promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(resultException);

        assertThat(BreakMppwContext.getReason(
                pluginRequest.getDatamartMnemonic(),
                pluginRequest.getSysCn()))
                .isEqualTo(MppwStopReason.FIRST_OFFSET_TIMEOUT);
        assertThat(BreakMppwContext.getNumberOfTasksByDatamart(pluginRequest.getDatamartMnemonic())).isEqualTo(1);
    }

    @NotNull
    private EdmlRequestContext createEdmlRequestContext() {
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext edmlRequestContext = new EdmlRequestContext(new RequestMetrics(), request, null, "env");
        edmlRequestContext.setDestinationEntity(Entity.builder()
                .name("pso")
                .schema("test")
                .entityType(EntityType.TABLE)
                .destination(sourceTypes)
                .build());
        edmlRequestContext.setSourceEntity(
                Entity.builder()
                        .name("upload_table")
                        .schema("test")
                        .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                        .build());
        return edmlRequestContext;
    }

    private void initStatusResultQueue(Queue<StatusQueryResult> statusResultQueue,
                                       long statusResultCount, long endOffset) {
        final LocalDateTime lastCommitTime = LocalDateTime.now(timeZone);
        final LocalDateTime lastMessageTime = LocalDateTime.now(timeZone);
        LongStream.range(0L, statusResultCount).forEach(key -> {
            statusResultQueue.add(createStatusQueryResult(
                    lastMessageTime.plus(msgProcessTimeoutMs * key, ChronoField.MILLI_OF_DAY.getBaseUnit()),
                    lastCommitTime.plus(msgCommitTimeoutMs * key, ChronoField.MILLI_OF_DAY.getBaseUnit()),
                    endOffset, key));
        });
    }

    private void initStatusResultQueueWithOffset(Queue<StatusQueryResult> adbStatusResultQueue,
                                                 long statusResultCount, long endOffset, long offset) {
        final LocalDateTime lastCommitTime = LocalDateTime.now(timeZone);
        final LocalDateTime lastMessageTime = LocalDateTime.now(timeZone);
        LongStream.range(0L, statusResultCount).forEach(key -> {
            adbStatusResultQueue.add(createStatusQueryResult(
                    lastMessageTime.plus(msgProcessTimeoutMs * key, ChronoField.MILLI_OF_DAY.getBaseUnit()),
                    lastCommitTime.plus(msgCommitTimeoutMs * key, ChronoField.MILLI_OF_DAY.getBaseUnit()),
                    endOffset, offset));
        });
    }

    private StatusQueryResult createStatusQueryResult(LocalDateTime lastMessageTime, LocalDateTime lastCommitTime, long endOffset, long offset) {
        StatusQueryResult statusQueryResult = new StatusQueryResult();
        KafkaPartitionInfo kafkaPartitionInfo = createKafkaPartitionInfo(lastMessageTime, lastCommitTime, endOffset, offset);
        statusQueryResult.setPartitionInfo(kafkaPartitionInfo);
        return statusQueryResult;
    }

    @NotNull
    private MppwKafkaParameter createKafkaParameter() {
        return MppwKafkaParameter.builder()
                .sysCn(1L)
                .datamart("test")
                .destinationTableName("test_tab")
                .uploadMetadata(UploadExternalEntityMetadata.builder()
                        .name("ext_tab")
                        .externalSchema("")
                        .uploadMessageLimit(1000)
                        .locationPath("kafka://kafka-1.dtm.local:9092/topic")
                        .format(ExternalTableFormat.AVRO)
                        .build())
                .brokers(Collections.singletonList(new KafkaBrokerInfo("kafka.host", 9092)))
                .topic("topic")
                .build();
    }

    @NotNull
    private KafkaPartitionInfo createKafkaPartitionInfo(LocalDateTime lastMessageTime,
                                                        LocalDateTime lastCommitTime,
                                                        long endOffset,
                                                        long offset) {
        return KafkaPartitionInfo.builder()
                .topic("topic")
                .start(0L)
                .end(endOffset)
                .lag(0L)
                .offset(offset)
                .lastMessageTime(lastMessageTime)
                .lastCommitTime(lastCommitTime)
                .partition(1)
                .build();
    }
}
