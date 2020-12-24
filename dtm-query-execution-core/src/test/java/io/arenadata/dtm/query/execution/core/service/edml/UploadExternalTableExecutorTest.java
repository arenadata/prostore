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

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadFailedExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadKafkaExecutor;
import io.arenadata.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.service.schema.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UploadExternalTableExecutorTest {

    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final List<EdmlUploadExecutor> uploadExecutors = Collections.singletonList(mock(UploadKafkaExecutor.class));
    private final EdmlUploadFailedExecutor uploadFailedExecutor = mock(UploadFailedExecutorImpl.class);
    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private final LogicalSchemaProvider logicalSchemaProvider = mock(LogicalSchemaProviderImpl.class);
    private UploadExternalTableExecutor uploadExternalTableExecutor;
    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
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
        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Datamart>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.EMPTY_LIST));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());
    }

    @Test
    void executeKafkaSuccessWithSysCnExists() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(sysCn);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(queryResult));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.succeededFuture());

        uploadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().succeeded());
        assertEquals(queryResult, promise.future().result());
    }

    @Test
    void executeKafkaSuccessWithoutSysCn() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 2L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(queryResult));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.succeededFuture());

        uploadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertEquals(queryResult, promise.future().result());
        assertEquals(context.getSysCn(), sysCn);
    }

    @Test
    void executeWriteNewOpError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        uploadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWriteOpSuccessError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(queryResult));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.failedFuture(new RuntimeException("")));

        uploadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        when(uploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.succeededFuture());

        when(uploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        uploadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWriteOpError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        uploadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeUploadFailedError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.succeededFuture());

        when(uploadFailedExecutor.execute(any())).thenReturn(Future.failedFuture(new RuntimeException("")));

        uploadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithNonexistingDestSource() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.getDestinationEntity().setDestination(new HashSet<>(Arrays.asList(SourceType.ADB,
                SourceType.ADG, SourceType.ADQM)));

        uploadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
}
