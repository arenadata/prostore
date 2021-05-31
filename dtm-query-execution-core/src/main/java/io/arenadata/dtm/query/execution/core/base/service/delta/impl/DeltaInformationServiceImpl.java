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
package io.arenadata.dtm.query.execution.core.base.service.delta.impl;

import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.exception.DeltaRangeInvalidException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationService;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class DeltaInformationServiceImpl implements DeltaInformationService {

    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public DeltaInformationServiceImpl(ServiceDbFacade serviceDbFacade) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    @Override
    public Future<Long> getCnToByDeltaDatetime(String datamart, LocalDateTime dateTime) {
        return Future.future(handler -> deltaServiceDao.getDeltaByDateTime(datamart, dateTime)
                .onSuccess(delta -> handler.handle(Future.succeededFuture(delta.getCnTo())))
                .onFailure(err -> handler.handle(Future.succeededFuture(-1L))));
    }

    @Override
    public Future<Long> getCnToByDeltaNum(String datamart, long num) {
        return Future.future(handler -> deltaServiceDao.getDeltaByNum(datamart, num)
                .onSuccess(delta -> handler.handle(Future.succeededFuture(delta.getCnTo())))
                .onFailure(err -> handler.handle(Future.succeededFuture(-1L))));
    }

    @Override
    public Future<Long> getCnToDeltaHot(String datamart) {
        return Future.future(handler -> deltaServiceDao.getDeltaHot(datamart)
                .onSuccess(deltaHot -> {
                    if (deltaHot != null && deltaHot.getCnTo() != null && deltaHot.getCnTo() >= 0) {
                        handler.handle(Future.succeededFuture(deltaHot.getCnTo()));
                    } else {
                        handleDeltaOk(datamart, handler);
                    }
                })
                .onFailure(handler::fail));
    }

    @Override
    public Future<SelectOnInterval> getCnFromCnToByDeltaNums(String datamart, long deltaFrom, long deltaTo) {
        return Future.future(handler -> CompositeFuture.join(deltaServiceDao.getDeltaByNum(datamart, deltaFrom), deltaServiceDao.getDeltaByNum(datamart, deltaTo))
                .onSuccess(ar -> {
                    Long cnFrom = ((OkDelta) ar.resultAt(0)).getCnFrom();
                    Long cnTo = ((OkDelta) ar.resultAt(1)).getCnTo();
                    handler.handle(Future.succeededFuture(new SelectOnInterval(cnFrom, cnTo)));
                })
                .onFailure(err -> {
                    val ex = new DeltaRangeInvalidException(String.format("Invalid delta range [%d, %d]", deltaFrom, deltaTo), err);
                    handler.handle(Future.failedFuture(ex));
                }));
    }

    @Override
    public Future<Long> getCnToDeltaOk(String datamart) {
        return Future.future(handler -> handleDeltaOk(datamart, handler));
    }

    private Future<OkDelta> handleDeltaOk(String datamart, Promise<Long> handler) {
        return deltaServiceDao.getDeltaOk(datamart)
                .onSuccess(okDelta -> {
                    Long cnTo = okDelta != null ? okDelta.getCnTo() : -1L;
                    handler.handle(Future.succeededFuture(cnTo));
                })
                .onFailure(handler::fail);
    }
}
