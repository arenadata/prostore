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
package io.arenadata.dtm.query.execution.core.metrics.service;

import io.arenadata.dtm.common.configuration.core.CoreConstants;
import io.arenadata.dtm.common.metrics.MetricsTopic;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.metrics.dto.MetricsSettings;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.time.LocalDateTime;

public abstract class AbstractMetricsService<T extends RequestMetrics> implements MetricsService<T> {

    private final MetricsProducer metricsProducer;
    private final MetricsSettings metricsSettings;

    public AbstractMetricsService(MetricsProducer metricsProducer,
                                  MetricsSettings metricsSettings) {
        this.metricsProducer = metricsProducer;
        this.metricsSettings = metricsSettings;
    }

    @Override
    public <R> Handler<AsyncResult<R>> sendMetrics(SourceType type,
                                                   SqlProcessingType actionType,
                                                   T requestMetrics,
                                                   Handler<AsyncResult<R>> handler) {
        if (!metricsSettings.isEnabled()) {
            return ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(ar.result()));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            };
        } else {
            return ar -> {
                updateMetrics(type, actionType, requestMetrics);
                if (ar.succeeded()) {
                    requestMetrics.setStatus(RequestStatus.SUCCESS);
                    metricsProducer.publish(MetricsTopic.ALL_EVENTS, requestMetrics);
                    handler.handle(Future.succeededFuture(ar.result()));
                } else {
                    requestMetrics.setStatus(RequestStatus.ERROR);
                    metricsProducer.publish(MetricsTopic.ALL_EVENTS, requestMetrics);
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            };
        }

    }

    @Override
    public Future<Void> sendMetrics(SourceType type,
                                    SqlProcessingType actionType,
                                    T requestMetrics) {
        if (!metricsSettings.isEnabled()) {
            return Future.succeededFuture();
        } else {
            return Future.future(promise -> {
                requestMetrics.setSourceType(type);
                requestMetrics.setActionType(actionType);
                metricsProducer.publish(MetricsTopic.ALL_EVENTS, requestMetrics);
                promise.complete();
            });
        }
    }

    private void updateMetrics(SourceType type, SqlProcessingType actionType, RequestMetrics metrics) {
        metrics.setActive(false);
        metrics.setEndTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID));
        metrics.setSourceType(type);
        metrics.setActionType(actionType);
    }
}
