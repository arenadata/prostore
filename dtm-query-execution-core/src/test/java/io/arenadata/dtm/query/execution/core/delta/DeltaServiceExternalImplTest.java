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
package io.arenadata.dtm.query.execution.core.delta;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.service.DeltaService;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.delta.service.impl.DeltaServiceExternalImpl;
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

class DeltaServiceExternalImplTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private DeltaService deltaService;
    private final DtmConfig dtmSettings = mock(CoreDtmSettings.class);
    private ZoneId timeZone;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(dtmSettings.getTimeZone()).thenReturn(ZoneId.of("UTC"));
        deltaService = new DeltaServiceExternalImpl(serviceDbFacade);
        timeZone = dtmSettings.getTimeZone();
    }

    @Test
    void getCnToDeltaHotgetDeltaHotSuccess(){
        Promise<Long> promise = Promise.promise();
        String datamart = "datamart";
        Long cnTo = 1L;
        HotDelta hotDelta = HotDelta.builder().cnTo(cnTo).build();

        when(deltaServiceDao.getDeltaHot(eq(datamart))).thenReturn(Future.succeededFuture(hotDelta));

        deltaService.getCnToDeltaHot(datamart)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertEquals(cnTo, promise.future().result());
    }

    @Test
    void getCnToDeltaHotgetDeltaOkSuccess(){
        Promise<Long> promise = Promise.promise();
        String datamart = "datamart";
        long cnTo = 1L;
        OkDelta okDelta = OkDelta.builder().cnTo(cnTo).build();

        when(deltaServiceDao.getDeltaHot(eq(datamart))).thenReturn(Future.succeededFuture(null));
        when(deltaServiceDao.getDeltaOk(eq(datamart))).thenReturn(Future.succeededFuture(okDelta));

        deltaService.getCnToDeltaHot(datamart)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertEquals(cnTo, promise.future().result());
    }

    @Test
    void getCnToDeltaHotAndDeltaOkNull(){
        Promise<Long> promise = Promise.promise();
        String datamart = "datamart";

        when(deltaServiceDao.getDeltaHot(eq(datamart))).thenReturn(Future.succeededFuture(null));
        when(deltaServiceDao.getDeltaOk(eq(datamart))).thenReturn(Future.succeededFuture(null));

        deltaService.getCnToDeltaHot(datamart)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertEquals(-1L, promise.future().result());
    }

    @Test
    void getCnToByDeltaNumSuccess(){
        Promise<Long> promise = Promise.promise();
        String datamart = "datamart";
        long deltaNum = 1;
        long cnTo = 1L;
        OkDelta okDelta = OkDelta.builder().cnTo(cnTo).build();

        when(deltaServiceDao.getDeltaByNum(eq(datamart), eq(deltaNum))).thenReturn(Future.succeededFuture(okDelta));

        deltaService.getCnToByDeltaNum(datamart, deltaNum)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertEquals(cnTo, promise.future().result());
    }

    @Test
    void getCnToByDeltaNumError(){
        Promise<Long> promise = Promise.promise();
        String datamart = "datamart";
        long deltaNum = 1;

        RuntimeException exOk = new DtmException("empty delta ok");
        when(deltaServiceDao.getDeltaByNum(eq(datamart), eq(deltaNum))).thenReturn(Future.failedFuture(exOk));

        deltaService.getCnToByDeltaNum(datamart, deltaNum)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertEquals(-1L, promise.future().result());
    }

    @Test
    void getCnToByDeltaDatetimeSuccess(){
        Promise<Long> promise = Promise.promise();
        String datamart = "datamart";
        LocalDateTime deltaDatetime = LocalDateTime.now(timeZone);
        long cnTo = 1L;
        OkDelta okDelta = OkDelta.builder().cnTo(cnTo).build();

        when(deltaServiceDao.getDeltaByDateTime(eq(datamart), eq(deltaDatetime))).thenReturn(Future.succeededFuture(okDelta));

        deltaService.getCnToByDeltaDatetime(datamart, deltaDatetime)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertEquals(cnTo, promise.future().result());
    }

    @Test
    void getCnToByDeltaDatetimeError(){
        Promise<Long> promise = Promise.promise();
        String datamart = "datamart";
        LocalDateTime deltaDatetime = LocalDateTime.now(timeZone);

        RuntimeException exOk = new DtmException("empty delta ok");
        when(deltaServiceDao.getDeltaByDateTime(eq(datamart), eq(deltaDatetime))).thenReturn(Future.failedFuture(exOk));

        deltaService.getCnToByDeltaDatetime(datamart, deltaDatetime)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertEquals(-1L, promise.future().result());
    }

    @Test
    void getIntervalSuccess(){
        Promise<SelectOnInterval> promise = Promise.promise();
        String datamart = "datamart";
        long cnFrom = 1L;
        OkDelta okDelta1 = OkDelta.builder().cnFrom(cnFrom).build();
        long cnTo = 2L;
        OkDelta okDelta2 = OkDelta.builder().cnTo(cnTo).build();

        when(deltaServiceDao.getDeltaByNum(eq(datamart), anyLong()))
                .thenReturn(Future.succeededFuture(okDelta1))
                .thenReturn(Future.succeededFuture(okDelta2));

        deltaService.getCnFromCnToByDeltaNums(datamart, 1, 2)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertEquals(new SelectOnInterval(cnFrom, cnTo), promise.future().result());
    }

    @Test
    void getIntervalgetFirstDeltaError(){
        Promise<SelectOnInterval> promise = Promise.promise();
        String datamart = "datamart";
        long cnTo = 2L;
        OkDelta okDelta2 = OkDelta.builder().cnFrom(cnTo).build();

        RuntimeException exOk = new DtmException("empty first delta ok");

        when(deltaServiceDao.getDeltaByNum(eq(datamart), anyLong()))
                .thenReturn(Future.failedFuture(exOk))
                .thenReturn(Future.succeededFuture(okDelta2));

        deltaService.getCnFromCnToByDeltaNums(datamart, 1, 2)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertNotNull(promise.future().cause());
        assertEquals("Invalid delta range [1, 2]", promise.future().cause().getMessage());
    }

    @Test
    void getIntervalgetSecondDeltaError(){
        Promise<SelectOnInterval> promise = Promise.promise();
        String datamart = "datamart";
        long cnFrom = 1L;
        OkDelta okDelta1 = OkDelta.builder().cnFrom(cnFrom).build();

        RuntimeException exOk = new DtmException("empty second delta ok");

        when(deltaServiceDao.getDeltaByNum(eq(datamart), anyLong()))
                .thenReturn(Future.succeededFuture(okDelta1))
                .thenReturn(Future.failedFuture(exOk));

        deltaService.getCnFromCnToByDeltaNums(datamart, 1, 2)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        assertNotNull(promise.future().cause());
        assertEquals("Invalid delta range [1, 2]", promise.future().cause().getMessage());
    }


}
