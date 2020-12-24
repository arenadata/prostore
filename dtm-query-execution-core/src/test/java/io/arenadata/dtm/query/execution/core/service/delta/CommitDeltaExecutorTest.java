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
package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.query.CommitDeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.delta.CommitDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.impl.CommitDeltaExecutor;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommitDeltaExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(CommitDeltaQueryResultFactory.class);
    private CommitDeltaExecutor commitDeltaExecutor;
    private QueryRequest req = new QueryRequest();
    private DeltaRecord delta = new DeltaRecord();
    private String datamart = "test_datamart";

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setDatamart(req.getDatamartMnemonic());
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx());
        req.setSql("COMMIT DELTA");
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));

        when(deltaServiceDao.writeDeltaHotSuccess(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaDate));

        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);

        commitDeltaExecutor.execute(deltaQuery, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertEquals(deltaDate, ((QueryResult) promise.future().result()).getResult()
                .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
    }

    @Test
    void executeWithDatetimeSuccess() {
        Promise promise = Promise.promise();
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx());
        String deltaInputDate = "2020-06-15 14:00:11";
        req.setSql("COMMIT DELTA '" + deltaInputDate + "'");

        final LocalDateTime deltaDate = LocalDateTime.parse(deltaInputDate,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .deltaDate(deltaDate)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));

        when(deltaServiceDao.writeDeltaHotSuccess(any(), any()))
                .thenReturn(Future.succeededFuture(deltaDate));

        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);

        commitDeltaExecutor.execute(deltaQuery, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(deltaDate, ((QueryResult) promise.future().result()).getResult()
                .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
    }

    @Test
    void executeWriteDeltaHotSuccessError() {
        req.setSql("COMMIT DELTA");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());

        RuntimeException exception = new RuntimeException("");

        when(deltaServiceDao.writeDeltaHotSuccess(eq(datamart)))
                .thenReturn(Future.failedFuture(exception));

        commitDeltaExecutor.execute(deltaQuery, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithDatetimeWriteDeltaHotSuccessError() {
        Promise promise = Promise.promise();
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx());
        String deltaInputDate = "2020-06-12 18:00:01";
        req.setSql("COMMIT DELTA '" + deltaInputDate + "'");

        final LocalDateTime deltaDate = LocalDateTime.parse(deltaInputDate,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);

        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .deltaDate(deltaDate)
                .build();

        when(deltaServiceDao.writeDeltaHotSuccess(eq(datamart), eq(deltaDate)))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        commitDeltaExecutor.execute(deltaQuery, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        req.setSql("COMMIT DELTA");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);

        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));

        when(deltaServiceDao.writeDeltaHotSuccess(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaDate));

        when(deltaQueryResultFactory.create(any()))
                .thenThrow(new RuntimeException(""));

        commitDeltaExecutor.execute(deltaQuery, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    private List<Map<String, Object>> createResult(LocalDateTime deltaDate) {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.DATE_TIME_FIELD),
                Collections.singletonList(deltaDate));
    }
}
