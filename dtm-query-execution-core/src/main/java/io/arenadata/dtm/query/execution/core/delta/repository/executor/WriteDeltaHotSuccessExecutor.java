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
package io.arenadata.dtm.query.execution.core.delta.repository.executor;

import io.arenadata.dtm.common.configuration.core.CoreConstants;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.delta.dto.Delta;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.exception.*;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Arrays;

import static io.arenadata.dtm.query.execution.core.delta.utils.DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER;

@Slf4j
@Component
public class WriteDeltaHotSuccessExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    private static final String CANT_WRITE_DELTA_HOT_MSG = "Can't write delta hot success";

    @Autowired
    public WriteDeltaHotSuccessExecutor(@Qualifier("zookeeperExecutor") ZookeeperExecutor executor,
                                        @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    public Future<LocalDateTime> execute(String datamart, LocalDateTime deltaHotDate) {
        val deltaStat = new Stat();
        Promise<LocalDateTime> resultPromise = Promise.promise();
        val ctx = new DeltaContext();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
                .map(bytes -> bytes == null ? new Delta() : deserializedDelta(bytes))
                .map(delta -> {
                    if (delta.getHot() == null) {
                        throw new DeltaIsAlreadyCommittedException();
                    }
                    ctx.setDelta(delta);
                    return delta;
                })
                .compose(delta -> delta.getOk() == null ?
                        Future.succeededFuture(delta) : createDeltaPaths(datamart, deltaHotDate, delta))
                .map(delta -> Delta.builder()
                        .ok(OkDelta.builder()
                                .deltaDate(deltaHotDate == null ?
                                        LocalDateTime.now(CoreConstants.CORE_ZONE_ID).withNano(0) : deltaHotDate)
                                .deltaNum(delta.getHot().getDeltaNum())
                                .cnFrom(delta.getHot().getCnFrom())
                                .cnTo(delta.getHot().getCnTo() == null ? delta.getHot().getCnFrom() : delta.getHot().getCnTo())
                                .build())
                        .build())
                .compose(delta -> executor.multi(getWriteDeltaHotSuccessOps(datamart, delta, deltaStat.getVersion())).map(delta))
                .onSuccess(delta -> {
                    log.debug("Write delta hot \"success\" by datamart[{}], deltaHotDate[{}] completed successfully",
                            datamart,
                            delta.getOk().getDeltaDate());
                    resultPromise.complete(delta.getOk().getDeltaDate());
                })
                .onFailure(error -> handleError(datamart, deltaHotDate, resultPromise, error));
        return resultPromise.future();
    }

    private void handleError(String datamart, LocalDateTime deltaHotDate, Promise<LocalDateTime> resultPromise, Throwable error) {
        val errMsg = String.format("Can't write delta hot \"success\" by datamart[%s], deltaDate[%s]",
                datamart,
                deltaHotDate);
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
    }

    private Future<Delta> createDeltaPaths(String datamart, LocalDateTime deltaHotDate, Delta delta) {
        if (deltaHotDate != null && isBeforeOrEqual(deltaHotDate, delta.getOk().getDeltaDate())) {
            return Future.failedFuture(
                    new DeltaUnableSetDateTimeException(DELTA_DATE_TIME_FORMATTER.format(deltaHotDate), DELTA_DATE_TIME_FORMATTER.format(delta.getOk().getDeltaDate())));
        } else if (deltaHotDate == null && isBeforeOrEqual(LocalDateTime.now(CoreConstants.CORE_ZONE_ID), delta.getOk().getDeltaDate())) {
            return Future.failedFuture(
                    new DeltaUnableSetDateTimeException(DELTA_DATE_TIME_FORMATTER.format(LocalDateTime.now(CoreConstants.CORE_ZONE_ID)), DELTA_DATE_TIME_FORMATTER.format(delta.getOk().getDeltaDate())));
        } else {
            return createDelta(datamart, delta);
        }
    }

    private boolean isBeforeOrEqual(LocalDateTime deltaHotDate, LocalDateTime actualOkDeltaDate) {
        return deltaHotDate.isBefore(actualOkDeltaDate) || deltaHotDate.isEqual(actualOkDeltaDate);
    }

    private Future<Delta> createDelta(String datamart, Delta delta) {
        return createDeltaDatePath(datamart, delta)
                .map(delta)
                .otherwise(error -> {
                    if (error instanceof KeeperException.NodeExistsException) {
                        return delta;
                    } else {
                        throw new DeltaException(CANT_WRITE_DELTA_HOT_MSG, error);
                    }
                })
                .compose(r ->
                        createDeltaDateTimePath(datamart, delta.getOk())
                                .map(delta)
                                .otherwise(error -> {
                                    if (error instanceof KeeperException.NodeExistsException) {
                                        return r;
                                    } else {
                                        throw new DeltaException(CANT_WRITE_DELTA_HOT_MSG, error);
                                    }
                                }))
                .compose(r ->
                        createDeltaDateNumPath(datamart, delta.getOk())
                                .map(delta)
                                .otherwise(error -> {
                                    if (error instanceof KeeperException.NodeExistsException) {
                                        return r;
                                    } else {
                                        throw new DeltaException(CANT_WRITE_DELTA_HOT_MSG, error);
                                    }
                                }));
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
