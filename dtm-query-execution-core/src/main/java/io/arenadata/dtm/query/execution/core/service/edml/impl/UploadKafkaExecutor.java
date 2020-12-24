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
package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.execution.core.configuration.properties.EdmlProperties;
import io.arenadata.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.service.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import io.arenadata.dtm.query.execution.core.service.impl.CheckColumnTypesServiceImpl;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.StatusRequest;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.*;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UploadKafkaExecutor implements EdmlUploadExecutor {

    public static final String MPPW_LOAD_ERROR_MESSAGE = "Runtime error mppw download!";
    private final DataSourcePluginService pluginService;
    private final MppwKafkaRequestFactory mppwKafkaRequestFactory;
    private final EdmlProperties edmlProperties;
    private final KafkaProperties kafkaProperties;
    private final Vertx vertx;
    private final DtmConfig dtmSettings;
    private final CheckColumnTypesService checkColumnTypesService;

    @Autowired
    public UploadKafkaExecutor(DataSourcePluginService pluginService,
                               MppwKafkaRequestFactory mppwKafkaRequestFactory,
                               EdmlProperties edmlProperties,
                               KafkaProperties kafkaProperties,
                               @Qualifier("coreVertx") Vertx vertx,
                               DtmConfig dtmSettings,
                               CheckColumnTypesService checkColumnTypesService) {
        this.pluginService = pluginService;
        this.mppwKafkaRequestFactory = mppwKafkaRequestFactory;
        this.edmlProperties = edmlProperties;
        this.kafkaProperties = kafkaProperties;
        this.vertx = vertx;
        this.dtmSettings = dtmSettings;
        this.checkColumnTypesService = checkColumnTypesService;
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        try {
            final Map<SourceType, Future<MppwStopFuture>> startMppwFutureMap = new HashMap<>();
            final Set<SourceType> destination = context.getDestinationEntity().getDestination();
            log.debug("Mppw loading into table [{}], datamart [{}], for plugins: {}",
                    context.getDestinationEntity().getName(),
                    context.getDestinationEntity().getSchema(),
                    destination);
            QueryParserRequest queryParserRequest = new QueryParserRequest(context.getRequest().getQueryRequest(),
                    context.getLogicalSchema());
            checkColumnTypesService.check(context.getDestinationEntity().getFields(), queryParserRequest)
                    .compose(areEqual -> areEqual ? mppwKafkaRequestFactory.create(context)
                            : Future.failedFuture(String.format(CheckColumnTypesServiceImpl.FAIL_CHECK_COLUMNS_PATTERN,
                            context.getDestinationEntity().getName())))
                    .onSuccess(mppwRequestContext -> {
                        destination.forEach(ds ->
                                startMppwFutureMap.put(ds, startMppw(ds, mppwRequestContext.copy(), context)));
                        checkPluginsMppwExecution(startMppwFutureMap, resultHandler);
                    })
                    .onFailure(fail -> resultHandler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error starting mppw download!", e);
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private Future<MppwStopFuture> startMppw(SourceType ds, MppwRequestContext mppwRequestContext, EdmlRequestContext context) {
        return Future.future((Promise<MppwStopFuture> promise) -> pluginService.mppw(ds, mppwRequestContext, ar -> {
            if (ar.succeeded()) {
                log.debug("A request has been sent for the plugin: {} to start mppw download: {}", ds, mppwRequestContext.getRequest());
                val statusRequestContext = createStatusRequestContext(mppwRequestContext, context);
                val mppwLoadStatusResult = MppwLoadStatusResult.builder()
                        .lastOffsetTime(LocalDateTime.now(dtmSettings.getTimeZone()))
                        .lastOffset(0L)
                        .build();
                vertx.setPeriodic(edmlProperties.getPluginStatusCheckPeriodMs(), timerId -> {
                    log.trace("Plugin status request: {} mppw downloads", ds);
                    getMppwLoadingStatus(ds, statusRequestContext)
                            .onComplete(chr -> {
                                if (chr.succeeded()) {
                                    //todo: Add error checking (try catch and so on)
                                    StatusQueryResult result = chr.result();
                                    updateMppwLoadStatus(mppwLoadStatusResult, result);
                                    if (isMppwLoadedSuccess(result)) {
                                        vertx.cancelTimer(timerId);
                                        MppwStopFuture stopFuture = MppwStopFuture.builder()
                                                .sourceType(ds)
                                                .future(stopMppw(ds, mppwRequestContext))
                                                .offset(result.getPartitionInfo().getOffset())
                                                .stopReason(MppwStopReason.OFFSET_RECEIVED)
                                                .build();
                                        promise.complete(stopFuture);
                                    } else if (isMppwLoadingInitFailure(mppwLoadStatusResult)) {
                                        vertx.cancelTimer(timerId);
                                        MppwStopFuture stopFuture = MppwStopFuture.builder()
                                                .sourceType(ds)
                                                .future(stopMppw(ds, mppwRequestContext))
                                                .cause(new RuntimeException(String.format("Plugin %s consumer failed to start", ds)))
                                                .stopReason(MppwStopReason.ERROR_RECEIVED)
                                                .build();
                                        promise.complete(stopFuture);
                                    } else if (isLastOffsetNotIncrease(mppwLoadStatusResult)) {
                                        vertx.cancelTimer(timerId);
                                        MppwStopFuture stopFuture = MppwStopFuture.builder()
                                                .sourceType(ds)
                                                .future(stopMppw(ds, mppwRequestContext))
                                                .cause(new RuntimeException(String.format("Plugin %s consumer offset stopped dead", ds)))
                                                .stopReason(MppwStopReason.ERROR_RECEIVED)
                                                .build();
                                        promise.complete(stopFuture);
                                    }
                                } else {
                                    log.error("Error getting plugin status: {}", ds, chr.cause());
                                    vertx.cancelTimer(timerId);
                                    promise.fail(chr.cause());
                                }
                            });
                });
            } else {
                log.error("Error starting loading mppw for plugin: {}", ds, ar.cause());
                MppwStopFuture stopFuture = MppwStopFuture.builder()
                        .sourceType(ds)
                        .future(stopMppw(ds, mppwRequestContext))
                        .cause(ar.cause())
                        .stopReason(MppwStopReason.ERROR_RECEIVED)
                        .build();
                promise.complete(stopFuture);
            }
        }));
    }

    private Future<StatusQueryResult> getMppwLoadingStatus(SourceType ds, StatusRequestContext statusRequestContext) {
        return Future.future((Promise<StatusQueryResult> promise) -> pluginService.status(ds, statusRequestContext, ar -> {
            if (ar.succeeded()) {
                StatusQueryResult queryResult = ar.result();
                log.trace("Plugin status received: {} mppw downloads: {}, on request: {}", ds, queryResult, statusRequestContext);
                promise.complete(queryResult);
            } else {
                promise.fail(ar.cause());
            }
        }));
    }

    private void updateMppwLoadStatus(MppwLoadStatusResult mppwLoadStatusResult, StatusQueryResult result) {
        if (result.getPartitionInfo().getOffset() > mppwLoadStatusResult.getLastOffset()) {
            mppwLoadStatusResult.setLastOffsetTime(LocalDateTime.now(dtmSettings.getTimeZone()));
            mppwLoadStatusResult.setLastOffset(result.getPartitionInfo().getOffset());
        }
    }

    private boolean isMppwLoadedSuccess(StatusQueryResult queryResult) {
        return queryResult.getPartitionInfo().getEnd().equals(queryResult.getPartitionInfo().getOffset())
                && queryResult.getPartitionInfo().getEnd() != 0
                && checkLastMessageTime(queryResult.getPartitionInfo().getLastMessageTime());
    }

    private boolean isMppwLoadingInitFailure(MppwLoadStatusResult mppwLoadStatusResult) {
        return mppwLoadStatusResult.getLastOffset() == 0L &&
                LocalDateTime.now(dtmSettings.getTimeZone()).isAfter(mppwLoadStatusResult.getLastOffsetTime()
                        .plus(edmlProperties.getFirstOffsetTimeoutMs(), ChronoField.MILLI_OF_DAY.getBaseUnit()));
    }

    private boolean isLastOffsetNotIncrease(MppwLoadStatusResult mppwLoadStatusResult) {
        return mppwLoadStatusResult.getLastOffset() != 0L &&
                LocalDateTime.now(dtmSettings.getTimeZone()).isAfter(mppwLoadStatusResult.getLastOffsetTime()
                        .plus(edmlProperties.getChangeOffsetTimeoutMs(), ChronoField.MILLI_OF_DAY.getBaseUnit()));
    }

    @NotNull
    private StatusRequestContext createStatusRequestContext(MppwRequestContext mppwRequestContext, EdmlRequestContext context) {
        val statusRequestContext = new StatusRequestContext(RequestMetrics.builder()
                .requestId(mppwRequestContext.getMetrics().getRequestId())
                .startTime(LocalDateTime.now(dtmSettings.getTimeZone()))
                .sourceType(mppwRequestContext.getMetrics().getSourceType())
                .actionType(SqlProcessingType.STATUS)
                .isActive(true)
                .status(RequestStatus.IN_PROCESS)
                .build(),
                new StatusRequest(context.getRequest().getQueryRequest()));
        statusRequestContext.getRequest().setTopic(mppwRequestContext.getRequest().getKafkaParameter().getTopic());
        return statusRequestContext;
    }

    private void checkPluginsMppwExecution(Map<SourceType, Future<MppwStopFuture>> startMppwFuturefMap, Handler<AsyncResult<QueryResult>> resultHandler) {
        final Map<SourceType, MppwStopFuture> mppwStopFutureMap = new HashMap<>();
        CompositeFuture.join(new ArrayList<>(startMppwFuturefMap.values()))
                .onComplete(startComplete -> {
                    if (startComplete.succeeded()) {
                        processStopFutures(mppwStopFutureMap, startComplete.result(), resultHandler);
                    } else {
                        log.error(MPPW_LOAD_ERROR_MESSAGE, startComplete.cause());
                        resultHandler.handle(Future.failedFuture(startComplete.cause()));
                    }
                });
    }

    private void processStopFutures(Map<SourceType, MppwStopFuture> mppwStopFutureMap,
                                    CompositeFuture startCompositeFuture, Handler<AsyncResult<QueryResult>> resultHandler) {
        List<Future<QueryResult>> stopMppwFutures = getStopMppwFutures(mppwStopFutureMap, startCompositeFuture);
        // This extra copy of futures to satisfy CompositeFuture.join signature, which require untyped Future
        CompositeFuture.join(new ArrayList<>(stopMppwFutures))
                .onComplete(stopComplete -> {
                    if (stopComplete.succeeded()){
                        if (isAllMppwPluginsHasEqualOffsets(mppwStopFutureMap)) {
                            resultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        } else {
                            String stopStatus = collectStatus(mppwStopFutureMap);
                            RuntimeException e = new RuntimeException(
                                    String.format("The offset of one of the plugins has changed: \n %s", stopStatus),
                                    stopComplete.cause());
                            log.error(MPPW_LOAD_ERROR_MESSAGE, e);
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        log.error("Error mppw stopping", stopComplete.cause());
                        resultHandler.handle(Future.failedFuture(stopComplete.cause()));
                    }
                });
    }

    private Future<QueryResult> stopMppw(SourceType ds, MppwRequestContext mppwRequestContext) {
        return Future.future((Promise<QueryResult> promise) -> {
            mppwRequestContext.getRequest().setIsLoadStart(false);
            log.debug("A request has been sent for the plugin: {} to stop loading mppw: {}", ds, mppwRequestContext.getRequest());
            pluginService.mppw(ds, mppwRequestContext, ar -> {
                if (ar.succeeded()) {
                    log.debug("Completed stopping mppw loading by plugin: {}", ds);
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    @NotNull
    private List<Future<QueryResult>> getStopMppwFutures(Map<SourceType, MppwStopFuture> mppwStopFutureMap, CompositeFuture startCompositeFuture) {
        startCompositeFuture.list().forEach(r -> {
            MppwStopFuture mppwResult = (MppwStopFuture) r;
            mppwStopFutureMap.putIfAbsent(mppwResult.getSourceType(), mppwResult);
        });
        return mppwStopFutureMap.values().stream().map(MppwStopFuture::getFuture).collect(Collectors.toList());
    }

    private String collectStatus(Map<SourceType, MppwStopFuture> mppwStopFutureMap) {
        return mppwStopFutureMap.values().stream().map(s ->
                String.format("Plugin: %s, status: %s, offset: %d, reason: %s",
                        s.sourceType.name(), s.stopReason.name(),
                        s.offset == null ? -1L : s.offset,
                        s.cause == null ? "" : s.cause.getMessage())
        ).collect(Collectors.joining("\n"));
    }

    private boolean isAllMppwPluginsHasEqualOffsets(Map<SourceType, MppwStopFuture> resultMap) {
        //check that the offset for each plugin has not changed
        if (!resultMap.isEmpty()) {
            Long offset = resultMap.values().stream().map(MppwStopFuture::getOffset).collect(Collectors.toList()).get(0);
            for (MppwStopFuture p : resultMap.values()) {
                if (p.getOffset() == null || !p.getOffset().equals(offset)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean checkLastMessageTime(LocalDateTime endMessageTime) {
        //todo: Remove this. Create normal checks.
        if (endMessageTime == null) {
            endMessageTime = LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        LocalDateTime endMessageTimeWithTimeout = endMessageTime.plus(kafkaProperties.getAdmin().getInputStreamTimeoutMs(),
                ChronoField.MILLI_OF_DAY.getBaseUnit());
        return endMessageTimeWithTimeout.isBefore(LocalDateTime.now(dtmSettings.getTimeZone()));
    }

    @Override
    public ExternalTableLocationType getUploadType() {
        return ExternalTableLocationType.KAFKA;
    }

    @Data
    @ToString
    @Builder
    private static class MppwStopFuture {
        private SourceType sourceType;
        private Future<QueryResult> future;
        private Long offset;
        private Throwable cause;
        private MppwStopReason stopReason;
    }

    @Data
    @Builder
    private static class MppwLoadStatusResult {
        private Long lastOffset;
        private LocalDateTime lastOffsetTime;
    }

    private enum MppwStopReason {
        OFFSET_RECEIVED, TIMEOUT_RECEIVED, ERROR_RECEIVED;
    }

}
