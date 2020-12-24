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
package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.GetDeltaByDateTimeExecutor;
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.DeltaException;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.DeltaNotExistException;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.DeltaNotFoundException;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class GetDeltaByDateTimeExecutorImpl extends DeltaServiceDaoExecutorHelper implements GetDeltaByDateTimeExecutor {

    public GetDeltaByDateTimeExecutorImpl(ZookeeperExecutor executor,
                                          @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<OkDelta> execute(String datamart, LocalDateTime dateTime) {
        val ctx = new DeltaContext();
        Promise<OkDelta> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart))
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                if (delta.getOk() != null) {
                    ctx.setDelta(delta);
                    val deltaDateTime = delta.getOk().getDeltaDate();
                    return deltaDateTime.isBefore(dateTime) || deltaDateTime.isEqual(dateTime);
                } else {
                    throw new DeltaNotExistException();
                }
            })
            .compose(isDeltaOk -> isDeltaOk ?
                Future.succeededFuture(ctx.getDelta().getOk())
                : findByDays(datamart, dateTime))
            .onSuccess(r -> {
                log.debug("get delta ok by datamart[{}], dateTime[{}] completed successfully: [{}]", datamart, dateTime, r);
                resultPromise.complete(r);
            })
            .onFailure(error -> {
                val errMsg = String.format("can't get delta ok on datamart[%s], dateTime[%s]",
                    datamart,
                    dateTime);
                log.error(errMsg, error.getMessage());
                if (error instanceof KeeperException.NoNodeException) {
                    resultPromise.fail(new DatamartNotExistsException(datamart));
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    private Future<OkDelta> findByDays(String datamart, LocalDateTime targetDateTime) {
        val date = targetDateTime.toLocalDate();
        Promise<OkDelta> resultPromise = Promise.promise();
        getDatamartDeltaDays(datamart, date)
            .onSuccess(days -> {
                if (days.size() > 0) {
                    val dayIterator = days.iterator();
                    findByDay(datamart,
                        dayIterator,
                        targetDateTime,
                        resultPromise);
                } else {
                    resultPromise.fail(new DeltaNotFoundException());
                }
            })
            .onFailure(resultPromise::fail);
        return resultPromise.future();
    }

    private Future<List<LocalDate>> getDatamartDeltaDays(String datamart, LocalDate date) {
        return executor.getChildren(getDeltaPath(datamart) + "/date")
            .map(daysStr -> daysStr.stream()
                .map(LocalDate::parse)
                .filter(day -> date.isAfter(day) || date.isEqual(day))
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList()));
    }

    private void findByDay(String datamart,
                           Iterator<LocalDate> dayIterator,
                           LocalDateTime targetDateTime,
                           Promise<OkDelta> resultPromise) {
        val day = dayIterator.next();
        getDeltaOkByMaxDeltaDateTime(datamart, day, targetDateTime)
            .onSuccess(okDelta -> {
                if (okDelta != null) {
                    resultPromise.complete(okDelta);
                } else if (dayIterator.hasNext()) {
                    findByDay(datamart, dayIterator, targetDateTime, resultPromise);
                } else {
                    resultPromise.fail(new DeltaNotExistException());
                }
            })
            .onFailure(resultPromise::fail);
    }

    private Future<OkDelta> getDeltaOkByMaxDeltaDateTime(String datamart,
                                                         LocalDate day,
                                                         LocalDateTime targetDateTime) {
        val targetTime = targetDateTime.toLocalTime();
        return executor.getChildren(getDeltaDatePath(datamart, day))
            .map(times -> times.stream()
                .map(LocalTime::parse)
                .filter(time -> targetTime.isAfter(time) || targetTime.equals(time))
                .max(Comparator.naturalOrder()))
            .compose(timeOpt -> timeOpt
                .map(localTime -> {
                    val dateTime = LocalDateTime.of(day, localTime);
                    return executor.getData(getDeltaDateTimePath(datamart, dateTime));
                }).orElse(Future.succeededFuture()))
            .map(this::getOkDelta);
    }

    private OkDelta getOkDelta(byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            return deserializedOkDelta(bytes);
        }
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return GetDeltaByDateTimeExecutor.class;
    }
}
