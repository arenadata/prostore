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
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.GetDeltaByNumExecutor;
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

@Slf4j
@Component
public class GetDeltaByNumExecutorImpl extends DeltaServiceDaoExecutorHelper implements GetDeltaByNumExecutor {

    public GetDeltaByNumExecutorImpl(ZookeeperExecutor executor,
                                        @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<OkDelta> execute(String datamart, Long deltaNum) {
        Promise<OkDelta> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart))
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                if (delta.getOk() == null || delta.getOk().getDeltaNum() < deltaNum) {
                    throw new DeltaNotExistException();
                }
                return delta.getOk();
            })
            .compose(okDelta -> okDelta.getDeltaNum() == deltaNum
                ? Future.succeededFuture(okDelta) : getDeltaByNumber(datamart, deltaNum))
            .onSuccess(r -> {
                log.debug("get delta ok by datamart[{}], deltaNum[{}] completed successfully: [{}]", datamart, deltaNum, r);
                resultPromise.complete(r);
            })
            .onFailure(error -> {
                val errMsg = String.format("can't get delta ok on datamart[%s], deltaNum[%d]",
                    datamart,
                    deltaNum);
                log.error(errMsg, error);
                if (error instanceof KeeperException.NoNodeException) {
                    resultPromise.fail(new DeltaNotFoundException(error));
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    private Future<OkDelta> getDeltaByNumber(String datamart, Long deltaNum) {
        return executor.getData(getDeltaNumPath(datamart, deltaNum))
            .map(this::deserializedOkDelta);
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return GetDeltaByNumExecutor.class;
    }
}
