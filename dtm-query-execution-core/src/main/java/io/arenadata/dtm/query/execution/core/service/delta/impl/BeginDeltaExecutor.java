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
package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.StatusEventPublisher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.BEGIN_DELTA;

@Component
@Slf4j
public class BeginDeltaExecutor implements DeltaExecutor, StatusEventPublisher {

    private final DeltaServiceDao deltaServiceDao;
    private final DeltaQueryResultFactory deltaQueryResultFactory;
    private final Vertx vertx;

    @Autowired
    public BeginDeltaExecutor(ServiceDbFacade serviceDbFacade,
                              @Qualifier("beginDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                              @Qualifier("coreVertx") Vertx vertx) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.vertx = vertx;
    }

    @Override
    public void execute(DeltaQuery deltaQuery, Handler<AsyncResult<QueryResult>> handler) {
        val beginDeltaQuery = (BeginDeltaQuery) deltaQuery;
        if (beginDeltaQuery.getDeltaNum() == null) {
            deltaServiceDao.writeNewDeltaHot(beginDeltaQuery.getDatamart())
                    .onSuccess(newDeltaHotNum -> {
                        try {
                            handler.handle(Future.succeededFuture(getDeltaQueryResult(newDeltaHotNum,
                                    beginDeltaQuery)));
                        } catch (Exception e) {
                            handler.handle(Future.failedFuture(e));
                        }
                    })
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } else {
            deltaServiceDao.writeNewDeltaHot(beginDeltaQuery.getDatamart(), beginDeltaQuery.getDeltaNum())
                    .onSuccess(newDeltaHotNum -> {
                        try {
                            handler.handle(Future.succeededFuture(getDeltaQueryResult(newDeltaHotNum,
                                    beginDeltaQuery)));
                        } catch (Exception e) {
                            handler.handle(Future.failedFuture(e));
                        }
                    })
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        }
    }

    private QueryResult getDeltaQueryResult(Long deltaHotNum, BeginDeltaQuery deltaQuery) {
        DeltaRecord deltaRecord = createDeltaRecord(deltaQuery.getDatamart(), deltaHotNum);
        publishStatus(StatusEventCode.DELTA_OPEN, deltaQuery.getDatamart(), deltaRecord);
        QueryResult res = deltaQueryResultFactory.create(deltaRecord);
        res.setRequestId(deltaQuery.getRequest().getRequestId());
        return res;
    }

    private DeltaRecord createDeltaRecord(String datamart, Long deltaNum) {
        return DeltaRecord.builder()
                .deltaNum(deltaNum)
                .datamart(datamart)
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return BEGIN_DELTA;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
