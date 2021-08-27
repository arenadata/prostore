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
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.check.dto.CheckSumRequestContext;
import io.arenadata.dtm.query.execution.core.check.exception.CheckSumException;
import io.arenadata.dtm.query.execution.core.check.service.impl.CheckSumTableService;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CheckSumTableServiceTest {

    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private CheckSumTableService checkSumTableService;
    private final static String DATAMART_MNEMONIC = "test";
    private final QueryRequest queryRequest = QueryRequest.builder().datamartMnemonic(DATAMART_MNEMONIC).build();

    @BeforeEach
    void setUp() {
        checkSumTableService = new CheckSumTableService(dataSourcePluginService, entityDao);
    }

    @Test
    void calcHashSumTableWithoutColumns() {
        Promise<Long> promise = Promise.promise();
        long hashInt32Value = 12345L;
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(10)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .build();

        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void calcHashSumTableWithColumns() {
        Promise<Long> promise = Promise.promise();
        long hashInt32Value = 12345L;
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(10)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .columns(new HashSet<>(Arrays.asList("f1", "f2")))
                .build();

        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void calcHashSumTable() {
        Promise<Long> promise = Promise.promise();
        long expectedHashSum = 7291435975718679096L;
        long hashInt32Value = 12345L;
        Set<SourceType> types = new HashSet<>(Collections.singletonList(SourceType.ADB));
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(0)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(types)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .build();
        when(dataSourcePluginService.getSourceTypes()).thenReturn(types);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(hashInt32Value, promise.future().result());
    }

    @Test
    void calcHashSumTableForSeveralSysCn() {
        Promise<Long> promise = Promise.promise();
        long expectedHashSum = 3630854813343887462L;
        long hashInt32Value = 12345L;
        Set<SourceType> types = new HashSet<>(Collections.singletonList(SourceType.ADB));
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(1)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(types)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .build();
        when(dataSourcePluginService.getSourceTypes()).thenReturn(types);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(hashInt32Value, promise.future().result());
    }

    @Test
    void calcHashSumTableNonEqualPluginsHashSum() {
        Promise<Long> promise = Promise.promise();
        long hashInt32Value = 12345L;

        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(0)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .build();

        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);

        doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(hashInt32Value);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(hashInt32Value);
            } else {
                return Future.succeededFuture(0L);
            }
        }).when(dataSourcePluginService).checkDataByHashInt32(any(), any(), any());

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals(CheckSumException.class, promise.future().cause().getClass());
    }

    @Test
    void calcHashSumAllTables() {
        Promise<Long> promise = Promise.promise();
        long hashInt32Value = 12345L;
        List<Entity> entities = Arrays.asList(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("test_table_2")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build());
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(3)
                .build();

        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(entities.stream()
                        .map(Entity::getName)
                        .collect(Collectors.toList())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(0).getName()))
                .thenReturn(Future.succeededFuture(entities.get(0)));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(1).getName()))
                .thenReturn(Future.succeededFuture(entities.get(1)));
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumForAllTables(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void calcHashSumAllTablesSeveralSysCn() {
        Promise<Long> promise = Promise.promise();
        long hashInt32Value = 12345L;
        long expectedHashSum = 3991422068848944482L;
        List<Entity> entities = Arrays.asList(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("test_table_2")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build());
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(1)
                .build();

        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(entities.stream()
                        .map(Entity::getName)
                        .collect(Collectors.toList())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(0).getName()))
                .thenReturn(Future.succeededFuture(entities.get(0)));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(1).getName()))
                .thenReturn(Future.succeededFuture(entities.get(1)));
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumForAllTables(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(hashInt32Value + hashInt32Value, promise.future().result());
    }

    @Test
    void calcHashSumAllTablesGetEntityError() {
        Promise<Long> promise = Promise.promise();
        long hashInt32Value = 12345L;
        List<Entity> entities = Arrays.asList(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("test_table_2")
                        .schema(DATAMART_MNEMONIC)
                        .entityType(EntityType.TABLE)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build());
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(3)
                .build();

        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(entities.stream()
                        .map(Entity::getName)
                        .collect(Collectors.toList())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(0).getName()))
                .thenReturn(Future.succeededFuture(entities.get(0)));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(1).getName()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumForAllTables(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void calcHashSumAllTablesNonEqualsHashSums() {
        Promise<Long> promise = Promise.promise();
        long hashInt32Value = 12345L;
        List<Entity> entities = Arrays.asList(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("test_table_2")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build());
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(3)
                .build();

        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(entities.stream()
                        .map(Entity::getName)
                        .collect(Collectors.toList())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(0).getName()))
                .thenReturn(Future.succeededFuture(entities.get(0)));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(1).getName()))
                .thenReturn(Future.succeededFuture(entities.get(1)));
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));
        doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(hashInt32Value);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(hashInt32Value);
            } else {
                return Future.succeededFuture(0L);
            }
        }).when(dataSourcePluginService).checkDataByHashInt32(any(), any(), any());

        checkSumTableService.calcCheckSumForAllTables(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }
}
