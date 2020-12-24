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

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.WriteDeltaHotSuccessExecutor;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.*;
import io.arenadata.dtm.query.execution.core.dto.delta.Delta;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Arrays;

@Slf4j
@Component
public class WriteDeltaHotSuccessExecutorImpl extends DeltaServiceDaoExecutorHelper implements WriteDeltaHotSuccessExecutor {

    private final DtmConfig dtmSettings;

    @Autowired
    public WriteDeltaHotSuccessExecutorImpl(ZookeeperExecutor executor,
                                            @Value("${core.env.name}") String envName,
                                            DtmConfig dtmSettings) {
        super(executor, envName);
        this.dtmSettings = dtmSettings;
    }

    @Override
    public Future<LocalDateTime> execute(String datamart, LocalDateTime deltaHotDate) {
        val deltaStat = new Stat();
        Promise<LocalDateTime> resultPromise = Promise.promise();
        val ctx = new DeltaContext();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
            .map(bytes -> bytes == null ? new Delta() : deserializedDelta(bytes))
            .map(delta -> {
                if (delta.getHot() == null) {
                    throw new DeltaNotStartedException();
                }
                ctx.setDelta(delta);
                return delta;
            })
            .compose(delta -> delta.getOk() == null ?
                Future.succeededFuture(delta) : createDeltaPaths(datamart, deltaHotDate, delta))
            .map(delta -> Delta.builder()
                .ok(OkDelta.builder()
                    .deltaDate(deltaHotDate == null ?
                            LocalDateTime.now(this.dtmSettings.getTimeZone()).withNano(0) : deltaHotDate)
                    .deltaNum(delta.getHot().getDeltaNum())
                    .cnFrom(delta.getHot().getCnFrom())
                    .cnTo(delta.getHot().getCnTo() == null ? delta.getHot().getCnFrom() : delta.getHot().getCnTo())
                    .build())
                .build())
            .compose(delta -> executor.multi(getWriteDeltaHotSuccessOps(datamart, delta, deltaStat.getVersion())).map(delta))
            .onSuccess(delta -> {
                log.debug("write delta hot \"success\" by datamart[{}], deltaHotDate[{}] completed successfully",
                    datamart,
                    delta.getOk().getDeltaDate());
                resultPromise.complete(delta.getOk().getDeltaDate());
            })
            .onFailure(error -> {
                val errMsg = String.format("can't write delta hot \"success\" by datamart[%s], deltaDate[%s]",
                    datamart,
                    deltaHotDate);
                log.error(errMsg, error);
                if (error instanceof KeeperException) {
                    if (error instanceof KeeperException.NotEmptyException) {
                        resultPromise.fail(new DeltaNotFinishedException(error));
                    } else if (error instanceof KeeperException.BadVersionException) {
                        resultPromise.fail(new DeltaAlreadyCommitedException(error));
                    } else {
                        resultPromise.fail(new DeltaException(errMsg, error));
                    }
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });

        return resultPromise.future();
    }

    private Future<Delta> createDeltaPaths(String datamart, LocalDateTime deltaHotDate, Delta delta) {
        if (deltaHotDate != null && deltaHotDate.isBefore(delta.getOk().getDeltaDate())) {
            return Future.failedFuture(new InvalidDeltaDateException());
        } else {
            return createDeltaDatePath(datamart, delta)
                .map(delta)
                .otherwise(error -> {
                    if (error instanceof KeeperException.NodeExistsException) {
                        return delta;
                    } else {
                        throw new DeltaException("Can't write delta hot success", error);
                    }
                })
                .compose(r ->
                    createDeltaDateTimePath(datamart, delta.getOk())
                        .map(delta)
                        .otherwise(error -> {
                            if (error instanceof KeeperException.NodeExistsException) {
                                return r;
                            } else {
                                throw new DeltaException("Can't write delta hot success", error);
                            }
                        }))
                .compose(r ->
                    createDeltaDateNumPath(datamart, delta.getOk())
                        .map(delta)
                        .otherwise(error -> {
                            if (error instanceof KeeperException.NodeExistsException) {
                                return r;
                            } else {
                                throw new DeltaException("Can't write delta hot success", error);
                            }
                        }));
        }
    }

    private Future<String> createDeltaDatePath(String datamart, Delta delta) {
        val deltaDateTime = delta.getOk().getDeltaDate();
        val deltaDateTimePath = getDeltaDatePath(datamart, deltaDateTime.toLocalDate());
        return executor.createEmptyPersistentPath(deltaDateTimePath);
    }

    private Future<String> createDeltaDateTimePath(String datamart, OkDelta okDelta) {
        val deltaDateTime = okDelta.getDeltaDate();
        val deltaDateTimePath = getDeltaDateTimePath(datamart, deltaDateTime);
        return executor.createPersistentPath(deltaDateTimePath, serializedOkDelta(okDelta));
    }

    private Future<String> createDeltaDateNumPath(String datamart, OkDelta okDelta) {
        val deltaNum = okDelta.getDeltaNum();
        val deltaNumPath = getDeltaNumPath(datamart, deltaNum);
        return executor.createPersistentPath(deltaNumPath, serializedOkDelta(okDelta));
    }

    private Iterable<Op> getWriteDeltaHotSuccessOps(String datamart, Delta delta, int deltaVersion) {
        return Arrays.asList(
            Op.delete(getDatamartPath(datamart) + "/run", -1),
            Op.delete(getDatamartPath(datamart) + "/block", -1),
            Op.setData(getDeltaPath(datamart), serializedDelta(delta), deltaVersion)
        );
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteDeltaHotSuccessExecutor.class;
    }

}
