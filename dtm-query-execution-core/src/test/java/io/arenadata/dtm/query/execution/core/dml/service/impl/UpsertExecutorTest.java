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
package io.arenadata.dtm.query.execution.core.dml.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaException;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequest;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.core.utils.TestUtils;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class UpsertExecutorTest {

    @Mock
    private DataSourcePluginService pluginService;
    @Mock
    private EntityDao entityDao;
    @Mock
    private DeltaServiceDao deltaServiceDao;
    @Mock
    private RestoreStateService restoreStateService;
    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;

    private UpsertExecutor upsertExecutor;
    private Entity entity;
    private DmlRequestContext requestContext;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);

        upsertExecutor = new UpsertExecutor(pluginService, serviceDbFacade, restoreStateService);
        entity = Entity.builder()
                .schema("datamart")
                .name("users")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .type(ColumnType.INT)
                                .primaryOrder(1)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("name")
                                .type(ColumnType.VARCHAR)
                                .nullable(true)
                                .build()))
                .destination(Collections.singleton(SourceType.ADB))
                .entityType(EntityType.TABLE)
                .build();

        String sql = "UPSERT INTO users(id, name) values(1, 'Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .isPrepare(false)
                .sql(sql)
                .build();
        requestContext = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();
    }

    @Test
    void upsertSuccess(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(error -> testContext.failNow(error.getCause()));
    }

    @Test
    void upsertIntoMatView(VertxTestContext testContext) {
        entity.setEntityType(EntityType.MATERIALIZED_VIEW);

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed to upsert into mat view"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Forbidden. Write operations allowed for logical tables only.", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertSuccessWithoutColumns(VertxTestContext testContext) {
        String sql = "UPSERT INTO users values(1, 'Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .isPrepare(false)
                .sql(sql)
                .build();
        DmlRequestContext context = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(error -> testContext.failNow(error.getCause()));
    }

    @Test
    void upsertWithNonExistedColumn(VertxTestContext testContext) {
        String sql = "UPSERT INTO users(id, name, non_existed_column) values(1, 'Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .isPrepare(false)
                .sql(sql)
                .build();
        DmlRequestContext context = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query contains non existed column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void upsertWithSystemColumn(VertxTestContext testContext) {
        String sql = "UPSERT INTO users(id, name, sys_from) values(1, 'Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .isPrepare(false)
                .sql(sql)
                .build();
        DmlRequestContext context = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query contains non existed column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void upsertWithoutNotNullColumn(VertxTestContext testContext) {
        String sql = "UPSERT INTO users(name) values('Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .isPrepare(false)
                .sql(sql)
                .build();
        DmlRequestContext context = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();

        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query doesn't contains non nullable column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void testWithNoDeltaHotFound(VertxTestContext testContext) {
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.failedFuture(new DeltaException("Delta hot not found")));

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because delta hot is not found"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Delta hot not found", error.getMessage())).completeNow());
    }

    @Test
    void testSourceTypeNotConfigured(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(false);

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because entity's source type is not configured"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Plugins: [ADB] for the table [users] datamart [datamart] are not configured", error.getMessage())).completeNow());
    }

    @Test
    void testNotUpsertNode(VertxTestContext testContext) {
        String sql = "INSERT INTO users(id, name) values(1, 'Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        requestContext.setSqlNode(sqlNode);

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not upsert sql node"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Not upsert operation.", error.getMessage())).completeNow());
    }

    @Test
    void testNotValuesSource(VertxTestContext testContext) {
        String sql = "UPSERT INTO users(id, name) SELECT 1";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        requestContext.setSqlNode(sqlNode);

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not upsert sql node"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Only VALUES source is allowed", ar.getMessage())).completeNow());
    }

    @Test
    void testNotInsert(VertxTestContext testContext) {
        String sql = "SELECT 1";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        requestContext.setSqlNode(sqlNode);

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not upsert sql node"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Unsupported sql node", error.getMessage())).completeNow());
    }

    @Test
    void testPluginLlwFailedWithDtmException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any())).thenReturn(Future.failedFuture(new DtmException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Llw failed", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Unexpected error: Llw failed", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithoutMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any())).thenReturn(Future.failedFuture(new RuntimeException()));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        upsertExecutor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Unexpected error", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testGetType() {
        assertEquals(DmlType.UPSERT, upsertExecutor.getType());
    }

}
