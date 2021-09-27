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
package io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.query.execution.core.base.configuration.CacheConfiguration;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeleteDeltaHotExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeleteWriteOperationExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.GetDeltaByDateTimeExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.GetDeltaByNumExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.GetDeltaHotExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.GetDeltaOkExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.GetDeltaWriteOperationsExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.WriteDeltaErrorExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.WriteDeltaHotSuccessExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.WriteNewDeltaHotExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.WriteNewOperationExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.WriteOperationErrorExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.WriteOperationSuccessExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaDaoExecutorRepository;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DeltaServiceDaoImpl implements DeltaServiceDao, DeltaDaoExecutorRepository {
    private final Map<Class<? extends DeltaDaoExecutor>, DeltaDaoExecutor> executorMap;
    private final CacheService<String, HotDelta> hotDeltaCacheService;
    private final CacheService<String, OkDelta> okDeltaCacheService;

    @Autowired
    public DeltaServiceDaoImpl(@Qualifier("hotDeltaCacheService") CacheService<String, HotDelta> hotDeltaCacheService,
                               @Qualifier("okDeltaCacheService") CacheService<String, OkDelta> okDeltaCacheService) {
        this.hotDeltaCacheService = hotDeltaCacheService;
        this.okDeltaCacheService = okDeltaCacheService;
        executorMap = new HashMap<>();
    }

    @Override
    public Future<Long> writeNewDeltaHot(String datamart) {
        return Future.future(promise -> writeNewDeltaHot(datamart, null)
                .onComplete(ar -> {
                    evictDeltaCaches(datamart);
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public Future<Long> writeNewDeltaHot(String datamart, Long deltaHotNum) {
        return Future.future(promise -> getExecutor(WriteNewDeltaHotExecutor.class).execute(datamart, deltaHotNum)
                .onComplete(ar -> {
                    evictDeltaCaches(datamart);
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public Future<LocalDateTime> writeDeltaHotSuccess(String datamart) {
        return Future.future(promise -> writeDeltaHotSuccess(datamart, null)
                .onComplete(ar -> {
                    evictDeltaCaches(datamart);
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public Future<LocalDateTime> writeDeltaHotSuccess(String datamart, LocalDateTime deltaHotDate) {
        return Future.future(promise -> getExecutor(WriteDeltaHotSuccessExecutor.class).execute(datamart, deltaHotDate)
                .onComplete(ar -> {
                    evictDeltaCaches(datamart);
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public Future<Void> writeDeltaError(String datamart, Long deltaHotNum) {
        return Future.future(promise -> getExecutor(WriteDeltaErrorExecutor.class).execute(datamart, deltaHotNum)
                .onComplete(ar -> {
                    evictDeltaCaches(datamart);
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public Future<Void> deleteDeltaHot(String datamart) {
        return Future.future(promise -> getExecutor(DeleteDeltaHotExecutor.class).execute(datamart)
                .onComplete(ar -> {
                    evictDeltaCaches(datamart);
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public Future<Long> writeNewOperation(DeltaWriteOpRequest operation) {
        return getExecutor(WriteNewOperationExecutor.class).execute(operation);
    }

    @Override
    public Future<Void> writeOperationSuccess(String datamart, long synCn) {
        return Future.future(promise -> getExecutor(WriteOperationSuccessExecutor.class).execute(datamart, synCn)
                .onComplete(ar -> {
                    evictDeltaCaches(datamart);
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public Future<Void> writeOperationError(String datamart, long synCn) {
        return getExecutor(WriteOperationErrorExecutor.class).execute(datamart, synCn);
    }

    @Override
    public Future<Void> deleteWriteOperation(String datamart, long synCn) {
        return getExecutor(DeleteWriteOperationExecutor.class).execute(datamart, synCn);
    }

    @Override
    public Future<OkDelta> getDeltaByNum(String datamart, long num) {
        return getExecutor(GetDeltaByNumExecutor.class).execute(datamart, num);
    }

    @Override
    public Future<OkDelta> getDeltaByDateTime(String datamart, LocalDateTime dateTime) {
        return getExecutor(GetDeltaByDateTimeExecutor.class).execute(datamart, dateTime);
    }

    @Override
    @Cacheable(value = CacheConfiguration.OK_DELTA_CACHE, key = "#datamart")
    public Future<OkDelta> getDeltaOk(String datamart) {
        return getExecutor(GetDeltaOkExecutor.class).execute(datamart);
    }

    @Override
    @Cacheable(value = CacheConfiguration.HOT_DELTA_CACHE, key = "#datamart")
    public Future<HotDelta> getDeltaHot(String datamart) {
        return getExecutor(GetDeltaHotExecutor.class).execute(datamart);
    }

    @Override
    public Future<List<DeltaWriteOp>> getDeltaWriteOperations(String datamart) {
        return getExecutor(GetDeltaWriteOperationsExecutor.class).execute(datamart);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DeltaDaoExecutor> T getExecutor(Class<T> executorInterface) {
        return (T) executorMap.get(executorInterface);
    }

    @Override
    public <T extends DeltaDaoExecutor> void addExecutor(T executor) {
        executorMap.put(executor.getExecutorInterface(), executor);
    }

    private void evictDeltaCaches(String datamart) {
        hotDeltaCacheService.remove(datamart);
        okDeltaCacheService.remove(datamart);
    }

}
