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

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.service.DeltaService;
import io.arenadata.dtm.query.execution.core.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.arenadata.dtm.query.execution.core.service.impl.DeltaServiceImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DeltaServiceImplTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private DeltaService deltaService;
    private DtmConfig dtmSettings = mock(CoreDtmSettings.class);
    private ZoneId timeZone;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(dtmSettings.getTimeZone()).thenReturn(ZoneId.of("UTC"));
        deltaService = new DeltaServiceImpl(serviceDbFacade);
        timeZone = dtmSettings.getTimeZone();
    }

    @Test
    void getCnToDeltaHotgetDeltaHotSuccess(){
        Promise promise = Promise.promise();
        String datamart = "datamart";
        Long cnTo = 1L;
        HotDelta hotDelta = HotDelta.builder().cnTo(cnTo).build();

        when(deltaServiceDao.getDeltaHot(eq(datamart))).thenReturn(Future.succeededFuture(hotDelta));

        deltaService.getCnToDeltaHot(datamart)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertEquals(cnTo, promise.future().result());
    }

    @Test
    void getCnToDeltaHotgetDeltaOkSuccess(){
        Promise promise = Promise.promise();
        String datamart = "datamart";
        Long cnTo = 1L;
        OkDelta okDelta = OkDelta.builder().cnTo(cnTo).build();

        RuntimeException ex = new RuntimeException("empty delta hot");
        when(deltaServiceDao.getDeltaHot(eq(datamart))).thenReturn(Future.failedFuture(ex));
        when(deltaServiceDao.getDeltaOk(eq(datamart))).thenReturn(Future.succeededFuture(okDelta));

        deltaService.getCnToDeltaHot(datamart)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertEquals(cnTo, promise.future().result());
    }

    @Test
    void getCnToDeltaHotError(){
        Promise promise = Promise.promise();
        String datamart = "datamart";

        RuntimeException exHot = new RuntimeException("empty delta hot");
        RuntimeException exOk = new RuntimeException("empty delta ok");
        when(deltaServiceDao.getDeltaHot(eq(datamart))).thenReturn(Future.failedFuture(exHot));
        when(deltaServiceDao.getDeltaOk(eq(datamart))).thenReturn(Future.failedFuture(exOk));

        deltaService.getCnToDeltaHot(datamart)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertEquals(-1L, promise.future().result());
    }

    @Test
    void getCnToByDeltaNumSuccess(){
        Promise promise = Promise.promise();
        String datamart = "datamart";
        long deltaNum = 1;
        Long cnTo = 1L;
        OkDelta okDelta = OkDelta.builder().cnTo(cnTo).build();

        when(deltaServiceDao.getDeltaByNum(eq(datamart), eq(deltaNum))).thenReturn(Future.succeededFuture(okDelta));

        deltaService.getCnToByDeltaNum(datamart, deltaNum)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertEquals(cnTo, promise.future().result());
    }

    @Test
    void getCnToByDeltaNumError(){
        Promise promise = Promise.promise();
        String datamart = "datamart";
        long deltaNum = 1;

        RuntimeException exOk = new RuntimeException("empty delta ok");
        when(deltaServiceDao.getDeltaByNum(eq(datamart), eq(deltaNum))).thenReturn(Future.failedFuture(exOk));

        deltaService.getCnToByDeltaNum(datamart, deltaNum)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertEquals(-1L, promise.future().result());
    }

    @Test
    void getCnToByDeltaDatetimeSuccess(){
        Promise promise = Promise.promise();
        String datamart = "datamart";
        LocalDateTime deltaDatetime = LocalDateTime.now(timeZone);
        Long cnTo = 1L;
        OkDelta okDelta = OkDelta.builder().cnTo(cnTo).build();

        when(deltaServiceDao.getDeltaByDateTime(eq(datamart), eq(deltaDatetime))).thenReturn(Future.succeededFuture(okDelta));

        deltaService.getCnToByDeltaDatetime(datamart, deltaDatetime)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertEquals(cnTo, promise.future().result());
    }

    @Test
    void getCnToByDeltaDatetimeError(){
        Promise promise = Promise.promise();
        String datamart = "datamart";
        LocalDateTime deltaDatetime = LocalDateTime.now(timeZone);

        RuntimeException exOk = new RuntimeException("empty delta ok");
        when(deltaServiceDao.getDeltaByDateTime(eq(datamart), eq(deltaDatetime))).thenReturn(Future.failedFuture(exOk));

        deltaService.getCnToByDeltaDatetime(datamart, deltaDatetime)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertEquals(-1L, promise.future().result());
    }

    @Test
    void getIntervalSuccess(){
        Promise promise = Promise.promise();
        String datamart = "datamart";
        Long cnFrom = 1L;
        OkDelta okDelta1 = OkDelta.builder().cnFrom(cnFrom).build();
        Long cnTo = 2L;
        OkDelta okDelta2 = OkDelta.builder().cnTo(cnTo).build();

        when(deltaServiceDao.getDeltaByNum(eq(datamart), anyLong()))
                .thenReturn(Future.succeededFuture(okDelta1))
                .thenReturn(Future.succeededFuture(okDelta2));

        deltaService.getCnFromCnToByDeltaNums(datamart, 1, 2)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertEquals(new SelectOnInterval(cnFrom, cnTo), promise.future().result());
    }

    @Test
    void getIntervalgetFirstDeltaError(){
        Promise promise = Promise.promise();
        String datamart = "datamart";
        Long cnTo = 2L;
        OkDelta okDelta2 = OkDelta.builder().cnFrom(cnTo).build();

        RuntimeException exOk = new RuntimeException("empty first delta ok");

        when(deltaServiceDao.getDeltaByNum(eq(datamart), anyLong()))
                .thenReturn(Future.failedFuture(exOk))
                .thenReturn(Future.succeededFuture(okDelta2));

        deltaService.getCnFromCnToByDeltaNums(datamart, 1, 2)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertNotNull(promise.future().cause());
        assertEquals("Invalid delta range [1, 2]", promise.future().cause().getMessage());
    }

    @Test
    void getIntervalgetSecondDeltaError(){
        Promise promise = Promise.promise();
        String datamart = "datamart";
        Long cnFrom = 1L;
        OkDelta okDelta1 = OkDelta.builder().cnFrom(cnFrom).build();

        RuntimeException exOk = new RuntimeException("empty second delta ok");

        when(deltaServiceDao.getDeltaByNum(eq(datamart), anyLong()))
                .thenReturn(Future.succeededFuture(okDelta1))
                .thenReturn(Future.failedFuture(exOk));

        deltaService.getCnFromCnToByDeltaNums(datamart, 1, 2)
                .onSuccess(success -> promise.complete(success))
                .onFailure(fail -> promise.fail(fail));

        assertNotNull(promise.future().cause());
        assertEquals("Invalid delta range [1, 2]", promise.future().cause().getMessage());
    }


}
