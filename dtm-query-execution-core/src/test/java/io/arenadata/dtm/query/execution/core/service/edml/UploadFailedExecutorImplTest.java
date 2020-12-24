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
package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.common.exception.CrashException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.RollbackRequestContextFactoryImpl;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadFailedExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UploadFailedExecutorImplTest {

    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final RollbackRequestContextFactory rollbackRequestContextFactory = mock(RollbackRequestContextFactoryImpl.class);
    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private EdmlUploadFailedExecutor uploadFailedExecutor;
    private QueryRequest queryRequest;
    private Set<SourceType> sourceTypes = new HashSet<>();
    private Entity sourceEntity;
    private Entity destEntity;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        sourceTypes.addAll(Arrays.asList(SourceType.ADB, SourceType.ADG));
        sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("upload_table")
                .schema("test")
                .externalTableSchema("")
                .build();
        destEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("pso")
                .schema("test")
                .destination(sourceTypes)
                .build();
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        uploadFailedExecutor = new UploadFailedExecutorImpl(deltaServiceDao,
                rollbackRequestContextFactory, pluginService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, null);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(1L);

        final RollbackRequestContext rollbackRequestContext = new RollbackRequestContext(
                new RequestMetrics(),
                RollbackRequest.builder()
                        .queryRequest(context.getRequest().getQueryRequest())
                        .datamart(context.getSourceEntity().getName())
                        .destinationTable(context.getDestinationEntity().getName())
                        .sysCn(context.getSysCn())
                        .entity(context.getDestinationEntity())
                        .build());

        when(rollbackRequestContextFactory.create(any()))
                .thenReturn(rollbackRequestContext);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(pluginService).rollback(any(), any(), any());

        when(deltaServiceDao.deleteWriteOperation(eq(sourceEntity.getSchema()), eq(context.getSysCn())))
                .thenReturn(Future.succeededFuture());

        uploadFailedExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executePluginRollbackError() {
        Promise promise = Promise.promise();
        uploadFailedExecutor = new UploadFailedExecutorImpl(deltaServiceDao,
                rollbackRequestContextFactory, pluginService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, null);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(1L);

        final RollbackRequestContext rollbackRequestContext = new RollbackRequestContext(
                new RequestMetrics(),
                RollbackRequest.builder()
                        .queryRequest(context.getRequest().getQueryRequest())
                        .datamart(context.getSourceEntity().getName())
                        .destinationTable(context.getDestinationEntity().getName())
                        .sysCn(context.getSysCn())
                        .entity(context.getDestinationEntity())
                        .build());

        when(rollbackRequestContextFactory.create(any()))
                .thenReturn(rollbackRequestContext);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(pluginService).rollback(any(), any(), any());

        uploadFailedExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof CrashException);
    }

    @Test
    void executeDeleteOperationError() {
        Promise promise = Promise.promise();
        uploadFailedExecutor = new UploadFailedExecutorImpl(deltaServiceDao,
                rollbackRequestContextFactory, pluginService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, null);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(1L);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(pluginService).rollback(any(), any(), any());

        when(deltaServiceDao.deleteWriteOperation(eq(sourceEntity.getSchema()), eq(context.getSysCn())))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        uploadFailedExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }
}
