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
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
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

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
public class DeleteExecutorTest {

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
    @Mock
    private LogicalSchemaProvider logicalSchemaProvider;

    private DeleteExecutor executor;
    private Entity entity;
    private DmlRequestContext requestContext;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);

        executor = new DeleteExecutor(pluginService, serviceDbFacade, restoreStateService, logicalSchemaProvider);

        entity = Entity.builder()
                .schema("datamart")
                .name("users")
                .destination(Collections.singleton(SourceType.ADB))
                .entityType(EntityType.TABLE)
                .build();

        String sql = "DELETE FROM users u where u.id = 1";
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
    public void deleteSuccess(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(ar -> testContext.failNow(ar.getCause()));
    }

    @Test
    public void testDeleteFromMatView(VertxTestContext testContext) {
        entity.setEntityType(EntityType.MATERIALIZED_VIEW);

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed to delete from mat view"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Forbidden. Write operations allowed for logical tables only.", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    public void testWithNoDeltaHotFound(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.failedFuture(new DeltaException("Delta hot not found")));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because delta hot is not found"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Delta hot not found", ar.getMessage())).completeNow());
    }

    @Test
    public void testWhenDatamartHasNoData(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(null));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(deltaServiceDao).getDeltaHot("datamart");
                    verify(deltaServiceDao).getDeltaOk("datamart");
                    verify(entityDao).getEntity("datamart", "users");
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(ar -> testContext.failNow("Should have been failed because delta ok is not found"));
    }

    @Test
    public void testWithNotDeleteNode(VertxTestContext testContext) {
        String sql = "SELECT 1";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        requestContext.setSqlNode(sqlNode);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because delta hot is not found"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Unsupported sql node", ar.getMessage())).completeNow());
    }

    @Test
    public void testSourceTypeNotConfigured(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(false);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because entity's source type is not configured"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Plugins: [ADB] for the table [users] datamart [datamart] are not configured", ar.getMessage())).completeNow());
    }

    @Test
    public void testPluginLlwFailedWithDtmException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.failedFuture(new DtmException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Llw failed", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    public void testPluginLlwFailedWithUnexpectedExceptionWithMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Unexpected error: Llw failed", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    public void testPluginLlwFailedWithUnexpectedExceptionWithoutMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.failedFuture(new RuntimeException()));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Unexpected error", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    public void testDeltaOkNotFound(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.failedFuture(new DeltaException("Delta ok not found")));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Delta ok not found", ar.getMessage());
                    verify(deltaServiceDao, never()).writeNewOperation(any());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    public void testDatamartsNotFound(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.failedFuture(new DtmException("Failed to get schema")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Failed to get schema", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    public void testDmlType() {
        assertEquals(executor.getType(), DmlType.DELETE);
    }

}
