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
package io.arenadata.dtm.query.execution.core.edml.mppw.service.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.execution.core.edml.configuration.EdmlProperties;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopFuture;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopReason;
import io.arenadata.dtm.query.execution.core.edml.mppw.factory.MppwErrorMessageFactory;
import io.arenadata.dtm.query.execution.core.edml.mppw.factory.MppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.EdmlUploadExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesServiceImpl;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.*;
import lombok.Builder;
import lombok.Data;
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

    private final DataSourcePluginService pluginService;
    private final MppwKafkaRequestFactory mppwKafkaRequestFactory;
    private final EdmlProperties edmlProperties;
    private final KafkaProperties kafkaProperties;
    private final Vertx vertx;
    private final DtmConfig dtmSettings;
    private final MppwErrorMessageFactory errorMessageFactory;
    private final CheckColumnTypesService checkColumnTypesService;

    @Autowired
    public UploadKafkaExecutor(DataSourcePluginService pluginService,
                               MppwKafkaRequestFactory mppwKafkaRequestFactory,
                               EdmlProperties edmlProperties,
                               KafkaProperties kafkaProperties,
                               @Qualifier("coreVertx") Vertx vertx,
                               DtmConfig dtmSettings,
                               MppwErrorMessageFactory errorMessageFactory,
                               CheckColumnTypesService checkColumnTypesService) {
        this.pluginService = pluginService;
        this.mppwKafkaRequestFactory = mppwKafkaRequestFactory;
        this.edmlProperties = edmlProperties;
        this.kafkaProperties = kafkaProperties;
        this.vertx = vertx;
        this.dtmSettings = dtmSettings;
        this.errorMessageFactory = errorMessageFactory;
        this.checkColumnTypesService = checkColumnTypesService;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return Future.future(promise -> {
            final Map<SourceType, Future<MppwStopFuture>> startMppwFutureMap = new HashMap<>();
            final Set<SourceType> destination = context.getDestinationEntity().getDestination();
            log.debug("Mppw loading into table [{}], datamart [{}], for plugins: {}",
                    context.getDestinationEntity().getName(),
                    context.getDestinationEntity().getSchema(),
                    destination);
            QueryParserRequest queryParserRequest = new QueryParserRequest(context.getSqlNode(),
                    context.getLogicalSchema());
            //TODO add checking for column names, and throw new ColumnNotExistsException if will be error
            checkColumnTypesService.check(context.getDestinationEntity().getFields(), queryParserRequest)
                    .compose(areEqual -> areEqual ? mppwKafkaRequestFactory.create(context)
                            : Future.failedFuture(new DtmException(String.format(CheckColumnTypesServiceImpl.FAIL_CHECK_COLUMNS_PATTERN,
                            context.getDestinationEntity().getName()))))
                    .onSuccess(kafkaRequest -> {
                        destination.forEach(ds -> startMppwFutureMap.put(ds,
                                startMppw(ds, context.getMetrics(), kafkaRequest.toBuilder().build())));
                        checkPluginsMppwExecution(startMppwFutureMap, context.getRequest().getQueryRequest().getRequestId(), promise);
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<MppwStopFuture> startMppw(SourceType ds,
                                             RequestMetrics metrics,
                                             MppwKafkaRequest kafkaRequest) {
        return Future.future((Promise<MppwStopFuture> promise) -> pluginService.mppw(ds, metrics, kafkaRequest)
                .onComplete(ar -> {
                    val mppwRequestWrapper = MppwRequestWrapper.builder()
                            .sourceType(ds)
                            .metrics(metrics)
                            .request(kafkaRequest)
                            .topic(kafkaRequest.getTopic())
                            .build();
                    if (ar.succeeded()) {
                        log.debug("A request has been sent for the plugin: {} to start mppw download: {}", ds, kafkaRequest);
                        val mppwLoadStatusResult = MppwLoadStatusResult.builder()
                                .lastOffsetTime(LocalDateTime.now(dtmSettings.getTimeZone()))
                                .lastOffset(0L)
                                .build();
                        mppwRequestWrapper.setLoadStatusResult(mppwLoadStatusResult);
                        sendStatusPeriodicaly(mppwRequestWrapper, promise);
                    } else {
                        MppwStopFuture stopFuture = MppwStopFuture.builder()
                                .sourceType(ds)
                                .future(stopMppw(mppwRequestWrapper))
                                .cause(new DtmException(String.format("Error starting loading mppw for plugin: %s", ds),
                                        ar.cause()))
                                .stopReason(MppwStopReason.ERROR_RECEIVED)
                                .build();
                        promise.complete(stopFuture);
                    }
                }));
    }

    private void sendStatusPeriodicaly(MppwRequestWrapper mppwRequestWrapper,
                                       Promise<MppwStopFuture> promise) {
        vertx.setTimer(edmlProperties.getPluginStatusCheckPeriodMs(), timerId -> {
            log.trace("Plugin status request: {} mppw downloads", mppwRequestWrapper.getSourceType());
            getMppwLoadingStatus(mppwRequestWrapper)
                    .onSuccess(statusQueryResult -> processMppwLoad(timerId, promise, mppwRequestWrapper, statusQueryResult))
                    .onFailure(fail -> {
                        vertx.cancelTimer(timerId);
                        promise.fail(new DtmException(
                                String.format("Error getting plugin status: %s", mppwRequestWrapper.getSourceType()),
                                fail));
                    });
        });
    }

    private void processMppwLoad(Long timerId,
                                 Promise<MppwStopFuture> promise,
                                 MppwRequestWrapper mppwRequestWrapper,
                                 StatusQueryResult statusQueryResult) {
        try {
            updateMppwLoadStatus(mppwRequestWrapper.getLoadStatusResult(), statusQueryResult);
            if (isMppwLoadedSuccess(statusQueryResult)) {
                log.debug("Plugin {} MPPW loaded successfully for request [{}]", mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getRequest().getRequestId());
                vertx.cancelTimer(timerId);
                MppwStopFuture stopFuture = MppwStopFuture.builder()
                        .sourceType(mppwRequestWrapper.getSourceType())
                        .future(stopMppw(mppwRequestWrapper))
                        .offset(statusQueryResult.getPartitionInfo().getOffset())
                        .stopReason(MppwStopReason.OFFSET_RECEIVED)
                        .build();
                promise.complete(stopFuture);
            } else if (isMppwLoadingInitFailure(mppwRequestWrapper.getLoadStatusResult())) {
                log.error("Plugin {} consumer failed to start for request [{}]", mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getRequest().getRequestId());
                vertx.cancelTimer(timerId);
                MppwStopFuture stopFuture = MppwStopFuture.builder()
                        .sourceType(mppwRequestWrapper.getSourceType())
                        .future(stopMppw(mppwRequestWrapper))
                        .cause(new DtmException(String.format("Plugin %s consumer failed to start",
                                mppwRequestWrapper.getSourceType())))
                        .stopReason(MppwStopReason.ERROR_RECEIVED)
                        .build();
                promise.complete(stopFuture);
            } else if (isLastOffsetNotIncrease(mppwRequestWrapper.getLoadStatusResult())) {
                log.error("Plugin {} last offset not increased for request [{}]", mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getRequest().getRequestId());
                vertx.cancelTimer(timerId);
                MppwStopFuture stopFuture = MppwStopFuture.builder()
                        .sourceType(mppwRequestWrapper.getSourceType())
                        .future(stopMppw(mppwRequestWrapper))
                        .cause(new DtmException(String.format("Plugin %s consumer offset stopped dead",
                                mppwRequestWrapper.getSourceType())))
                        .stopReason(MppwStopReason.ERROR_RECEIVED)
                        .build();
                promise.complete(stopFuture);
            } else {
                sendStatusPeriodicaly(mppwRequestWrapper, promise);
            }
        } catch (Exception e) {
            vertx.cancelTimer(timerId);
            MppwStopFuture stopFuture = MppwStopFuture.builder()
                    .sourceType(mppwRequestWrapper.getSourceType())
                    .future(stopMppw(mppwRequestWrapper))
                    .cause(new DtmException(String.format("Error in processing mppw by plugin %s",
                            mppwRequestWrapper.getSourceType()), e))
                    .stopReason(MppwStopReason.ERROR_RECEIVED)
                    .build();
            promise.complete(stopFuture);
        }
    }

    private Future<StatusQueryResult> getMppwLoadingStatus(MppwRequestWrapper mppwRequestWrapper) {
        return Future.future((Promise<StatusQueryResult> promise) ->
                pluginService.status(mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getMetrics(), mppwRequestWrapper.getTopic())
                        .onSuccess(queryResult -> {
                            log.trace("Plugin status received: {} mppw downloads: {}, on topic: {}",
                                    mppwRequestWrapper.getSourceType(),
                                    queryResult,
                                    mppwRequestWrapper.getTopic());
                            promise.complete(queryResult);
                        })
                        .onFailure(promise::fail));
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

    private void checkPluginsMppwExecution(Map<SourceType, Future<MppwStopFuture>> startMppwFuturefMap,
                                           UUID requestId,
                                           Promise<QueryResult> promise) {
        final Map<SourceType, MppwStopFuture> mppwStopFutureMap = new HashMap<>();
        CompositeFuture.join(new ArrayList<>(startMppwFuturefMap.values()))
                .onSuccess(startResult -> processStopFutures(mppwStopFutureMap, startResult, requestId, promise))
                .onFailure(promise::fail);
    }

    private void processStopFutures(Map<SourceType, MppwStopFuture> mppwStopFutureMap,
                                    CompositeFuture startCompositeFuture,
                                    UUID requestId,
                                    Promise<QueryResult> promise) {
        List<Future<QueryResult>> stopMppwFutures = getStopMppwFutures(mppwStopFutureMap, startCompositeFuture);
        // This extra copy of futures to satisfy CompositeFuture.join signature, which require untyped Future

        CompositeFuture.join(new ArrayList<>(stopMppwFutures))
                .onComplete(stopComplete -> {
                    if (stopComplete.succeeded()) {
                        if (isAllMppwPluginsHasEqualOffsets(mppwStopFutureMap)) {
                            log.debug("MPPW load successfully finished for request [{}]", requestId);
                            promise.complete(QueryResult.emptyResult());
                        } else {
                            failMppw(mppwStopFutureMap, requestId, promise, stopComplete.cause());
                        }
                    } else {
                        failMppw(mppwStopFutureMap, requestId, promise, stopComplete.cause());
                    }
                });
    }

    private void failMppw(Map<SourceType, MppwStopFuture> mppwStopFutureMap, UUID requestId, Promise<QueryResult> promise, Throwable cause) {
        String stopStatus = collectStatus(mppwStopFutureMap);
        RuntimeException e = new DtmException(
                String.format("The offset of one of the plugins has changed: %n %s", stopStatus),
                cause);
        log.error("MPPW load failed for request [{}], cause: {}", requestId, stopStatus);
        promise.fail(e);
    }

    private Future<QueryResult> stopMppw(MppwRequestWrapper mppwRequestWrapper) {
        return Future.future((Promise<QueryResult> promise) -> {
            mppwRequestWrapper.getRequest().setIsLoadStart(false);
            log.debug("A request has been sent for the plugin: {} to stop loading mppw: {}",
                    mppwRequestWrapper.getSourceType(),
                    mppwRequestWrapper.getRequest());
            pluginService.mppw(mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getMetrics(), mppwRequestWrapper.getRequest())
                    .onSuccess(queryResult -> {
                        log.debug("Completed stopping mppw loading by plugin: {}", mppwRequestWrapper.getSourceType());
                        promise.complete(queryResult);
                    })
                    .onFailure(promise::fail);
        });
    }

    @NotNull
    private List<Future<QueryResult>> getStopMppwFutures(Map<SourceType, MppwStopFuture> mppwStopFutureMap,
                                                         CompositeFuture startCompositeFuture) {
        startCompositeFuture.list().forEach(r -> {
            MppwStopFuture mppwResult = (MppwStopFuture) r;
            mppwStopFutureMap.putIfAbsent(mppwResult.getSourceType(), mppwResult);
        });
        return mppwStopFutureMap.values().stream().map(MppwStopFuture::getFuture).collect(Collectors.toList());
    }

    private String collectStatus(Map<SourceType, MppwStopFuture> mppwStopFutureMap) {
        return mppwStopFutureMap.values().stream()
                .map(errorMessageFactory::create)
                .collect(Collectors.joining("\n"));
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
    @Builder
    private static class MppwLoadStatusResult {
        private Long lastOffset;
        private LocalDateTime lastOffsetTime;
    }

    @Data
    @Builder
    private static class MppwRequestWrapper {
        private SourceType sourceType;
        private RequestMetrics metrics;
        private MppwKafkaRequest request;
        private String topic;
        private MppwLoadStatusResult loadStatusResult;
    }

}
