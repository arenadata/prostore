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
package io.arenadata.dtm.query.execution.core.check;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.*;
import io.arenadata.dtm.common.reader.*;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.*;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.check.exception.CheckSumException;
import io.arenadata.dtm.query.execution.core.check.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.core.check.factory.impl.CheckQueryResultFactoryImpl;
import io.arenadata.dtm.query.execution.core.check.service.CheckSumTableService;
import io.arenadata.dtm.query.execution.core.check.service.impl.*;
import io.arenadata.dtm.query.execution.core.delta.dto.*;
import io.arenadata.dtm.query.execution.core.delta.exception.*;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.vertx.core.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.stream.*;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CheckSumExecutorTest {

    private final EntityDao entityDao = mock(EntityDao.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDao.class);
    private final CheckSumTableService checkSumTableService = mock(CheckSumTableServiceImpl.class);
    private final SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);

    private final CheckQueryResultFactory queryResultFactory = new CheckQueryResultFactoryImpl();
    private final static String DATAMART_MNEMONIC = "test";
    private final Long okDeltaNum = 0L;
    private final Long hotDeltaNum = 1L;
    private final Long hashSum = 12345L;
    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());

    private CheckSumExecutor checkSumExecutor;
    private Entity entity;
    private QueryRequest queryRequest;
    private CheckContext checkContext;
    private OkDelta okDelta;
    private HotDelta hotDelta;

    @BeforeEach
    void setUp() {
        checkSumExecutor = new CheckSumExecutor(deltaServiceDao, entityDao, checkSumTableService, queryResultFactory);
        entity = Entity.builder()
                .schema(DATAMART_MNEMONIC)
                .entityType(EntityType.TABLE)
                .destination(SOURCE_TYPES)
                .name("test_table")
                .fields(Arrays.asList(EntityField.builder()
                                .name("f1")
                                .build(),
                        EntityField.builder()
                                .name("f2")
                                .build(),
                        EntityField.builder()
                                .name("f3")
                                .build()))
                .build();
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);
        okDelta = OkDelta.builder()
                .deltaNum(okDeltaNum)
                .cnFrom(0)
                .cnTo(1)
                .build();
        hotDelta = HotDelta.builder()
                .deltaNum(hotDeltaNum)
                .cnFrom(0L)
                .cnTo(1L)
                .build();

        when(entityDao.getEntity(DATAMART_MNEMONIC, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
        when(sqlCheckSum.getDeltaNum()).thenReturn(okDeltaNum);
        when(sqlCheckSum.getTable()).thenReturn(entity.getName());
    }

    @Test
    void executeNonEqualHotDelNum() {
        Promise<QueryResult> promise = Promise.promise();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.succeededFuture(okDelta));
        when(checkSumTableService.calcCheckSumTable(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum.toString(), promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeEqualHotDelNum() {
        Promise<QueryResult> promise = Promise.promise();

        hotDelta.setDeltaNum(okDeltaNum);

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(checkSumTableService.calcCheckSumTable(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum.toString(), promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeNullHotDelta() {
        Promise<QueryResult> promise = Promise.promise();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(null));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.succeededFuture(okDelta));
        when(checkSumTableService.calcCheckSumTable(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum.toString(), promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeNullCnToDelNum() {
        Promise<QueryResult> promise = Promise.promise();

        hotDelta.setDeltaNum(okDeltaNum);
        hotDelta.setCnTo(null);

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.succeededFuture(okDelta));
        when(checkSumTableService.calcCheckSumTable(any()))
                .thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof DeltaIsEmptyException);
        assertEquals(new DeltaIsEmptyException(0).getMessage(), promise.future().cause().getMessage());
    }

    @Test
    void executeWithGetDeltaHotError() {
        Promise<QueryResult> promise = Promise.promise();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.failedFuture(new DtmException("")));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void executeCheckSumTableError() {
        Promise<QueryResult> promise = Promise.promise();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.succeededFuture(okDelta));
        when(checkSumTableService.calcCheckSumTable(any()))
                .thenReturn(Future.failedFuture(new CheckSumException(entity.getName())));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals(CheckSumException.class, promise.future().cause().getClass());
    }

    @Test
    void executeNullCheckSumTableSuccess() {
        Promise<QueryResult> promise = Promise.promise();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.succeededFuture(okDelta));
        when(checkSumTableService.calcCheckSumTable(any())).thenReturn(Future.succeededFuture(null));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertNull(promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeWithNonTableNorMatViewEntity() {
        Promise<QueryResult> promise = Promise.promise();
        entity.setEntityType(EntityType.VIEW);

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.succeededFuture(okDelta));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals(EntityNotExistsException.class, promise.future().cause().getClass());
    }

    @Test
    void executeWithMaterializedViewEntity() {
        Promise<QueryResult> promise = Promise.promise();
        entity.setEntityType(EntityType.MATERIALIZED_VIEW);

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.succeededFuture(okDelta));
        when(checkSumTableService.calcCheckSumTable(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum.toString(), promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeWithGetDeltaNumError() {
        Promise<QueryResult> promise = Promise.promise();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.failedFuture(new DeltaNotFoundException()));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void executeCheckSumAllTableSuccess() {
        Promise<QueryResult> promise = Promise.promise();

        when(sqlCheckSum.getTable()).thenReturn(null);
        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.succeededFuture(okDelta));
        when(checkSumTableService.calcCheckSumForAllTables(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum.toString(), promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeCheckSumAllTableError() {
        Promise<QueryResult> promise = Promise.promise();

        when(sqlCheckSum.getTable()).thenReturn(null);
        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, okDeltaNum))
                .thenReturn(Future.succeededFuture(okDelta));
        when(checkSumTableService.calcCheckSumForAllTables(any()))
                .thenReturn(Future.failedFuture(new CheckSumException(entity.getName())));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals(CheckSumException.class, promise.future().cause().getClass());
    }

}