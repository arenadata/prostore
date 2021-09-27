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
package io.arenadata.dtm.query.execution.core.delta.service;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaRecord;
import io.arenadata.dtm.query.execution.core.delta.dto.query.CommitDeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.service.DeltaService;
import io.arenadata.dtm.query.execution.core.delta.service.StatusEventPublisher;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

import static io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaAction.COMMIT_DELTA;

@Component
@Slf4j
public class CommitDeltaService implements DeltaService, StatusEventPublisher {

    private static final String ERR_GETTING_QUERY_RESULT_MSG = "Error creating commit delta result";
    private final Vertx vertx;
    private final DeltaServiceDao deltaServiceDao;
    private final DeltaQueryResultFactory deltaQueryResultFactory;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public CommitDeltaService(ServiceDbFacade serviceDbFacade,
                              @Qualifier("commitDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                              @Qualifier("coreVertx") Vertx vertx,
                              EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.vertx = vertx;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return commitDelta(deltaQuery);
    }

    private Future<QueryResult> commitDelta(DeltaQuery deltaQuery) {
        return Future.future(promise -> {
            val commitDeltaQuery = (CommitDeltaQuery) deltaQuery;
            if (commitDeltaQuery.getDeltaDate() == null) {
                writeDeltaHot(commitDeltaQuery).onComplete(promise);
            } else {
                writeDeltaHotByDate(commitDeltaQuery).onComplete(promise);
            }
        });
    }

    private Future<QueryResult> writeDeltaHotByDate(CommitDeltaQuery commitDeltaQuery) {
        return Future.future(promise -> {
            try {
                evictQueryTemplateCacheService.evictByDatamartName(commitDeltaQuery.getDatamart());
            } catch (Exception e) {
                promise.fail(new DtmException("Evict cache error"));
            }
            deltaServiceDao.writeDeltaHotSuccess(commitDeltaQuery.getDatamart(), commitDeltaQuery.getDeltaDate())
                    .onSuccess(deltaDate -> {
                        try {
                            promise.complete(getQueryResult(commitDeltaQuery, deltaDate));
                        } catch (Exception e) {
                            promise.fail(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e));
                        }
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<QueryResult> writeDeltaHot(CommitDeltaQuery commitDeltaQuery) {
        return Future.future(promise -> {
            try {
                evictQueryTemplateCacheService.evictByDatamartName(commitDeltaQuery.getDatamart());
            } catch (Exception e) {
                promise.fail(new DtmException("Evict cache error"));
            }
            deltaServiceDao.writeDeltaHotSuccess(commitDeltaQuery.getDatamart())
                    .onSuccess(deltaDate -> {
                        try {
                            promise.complete(getQueryResult(commitDeltaQuery, deltaDate));
                        } catch (Exception e) {
                            promise.fail(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e));
                        }
                    })
                    .onFailure(promise::fail);
        });
    }

    private QueryResult getQueryResult(CommitDeltaQuery commitDeltaQuery, LocalDateTime deltaDate) {
        DeltaRecord deltaRecord = createDeltaRecord(commitDeltaQuery.getDatamart(), deltaDate);
        publishStatus(StatusEventCode.DELTA_CLOSE, commitDeltaQuery.getDatamart(), deltaRecord);
        QueryResult res = deltaQueryResultFactory.create(deltaRecord);
        res.setRequestId(commitDeltaQuery.getRequest().getRequestId());
        return res;
    }

    private DeltaRecord createDeltaRecord(String datamart, LocalDateTime deltaDate) {
        return DeltaRecord.builder()
                .datamart(datamart)
                .deltaDate(deltaDate)
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return COMMIT_DELTA;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
