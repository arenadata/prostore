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
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.calcite.core.service.impl.DeltaQueryPreprocessorImpl;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.service.dml.LogicViewReplacer;
import io.arenadata.dtm.query.execution.core.service.dml.impl.LogicViewReplacerImpl;
import io.arenadata.dtm.query.execution.core.service.edml.impl.DownloadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.impl.DownloadKafkaExecutor;
import io.arenadata.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.service.schema.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DownloadExternalTableExecutorTest {
    public static final String SELECT_SQL = "select id, lst_nam FROM test.pso";
    private final LogicalSchemaProvider logicalSchemaProvider = mock(LogicalSchemaProviderImpl.class);
    private final DeltaQueryPreprocessor deltaQueryPreprocessor = mock(DeltaQueryPreprocessorImpl.class);
    private final List<EdmlDownloadExecutor> downloadExecutors = Arrays.asList(mock(DownloadKafkaExecutor.class));
    private DownloadExternalTableExecutor downloadExternalTableExecutor;
    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private QueryRequest queryRequest;
    private Entity destEntity;
    private Entity sourceEntity;
    private List<Datamart> schema = Collections.emptyList();
    private LogicViewReplacer logicViewReplacer = mock(LogicViewReplacerImpl.class);

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<String>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(SELECT_SQL));
            return null;
        }).when(logicViewReplacer).replace(any(), any(), any());

        destEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("download_table")
                .schema("test")
                .externalTableSchema("")
                .build();

        sourceEntity = Entity.builder()
                .schema("test")
                .name("pso")
                .entityType(EntityType.TABLE)
                .build();
    }

    @Test
    void executeKafkaExecutorSuccess() {
        Promise promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
                deltaQueryPreprocessor, downloadExecutors, logicViewReplacer);
        String insertSql = "insert into test.download_table " + SELECT_SQL;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        QueryRequest copyRequest = context.getRequest().getQueryRequest();
        copyRequest.setDeltaInformations(Collections.emptyList());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Datamart>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());

        when(deltaQueryPreprocessor.process(any()))
                .thenReturn(Future.succeededFuture(copyRequest));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(downloadExecutors.get(0)).execute(any(), any());

        downloadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertNotNull(context.getRequest().getQueryRequest().getDeltaInformations());
    }

    @Test
    void executeKafkaGetLogicalSchemaError() {
        Promise promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
                deltaQueryPreprocessor, downloadExecutors, logicViewReplacer);
        String insertSql = "insert into test.download_table " + SELECT_SQL;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        QueryRequest copyRequest = context.getRequest().getQueryRequest();
        copyRequest.setDeltaInformations(Collections.emptyList());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<JsonObject>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());

        downloadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaDeltaProcessError() {
        Promise promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
                deltaQueryPreprocessor, downloadExecutors, logicViewReplacer);
        String insertSql = "insert into test.download_table " + SELECT_SQL;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        QueryRequest copyRequest = context.getRequest().getQueryRequest();
        copyRequest.setDeltaInformations(Collections.emptyList());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Datamart>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());

        when(deltaQueryPreprocessor.process(any()))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        downloadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaExecutorError() {
        Promise promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
                deltaQueryPreprocessor, downloadExecutors, logicViewReplacer);
        String insertSql = "insert into test.download_table " + SELECT_SQL;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        QueryRequest copyRequest = context.getRequest().getQueryRequest();
        copyRequest.setDeltaInformations(Collections.emptyList());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Datamart>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());

        when(deltaQueryPreprocessor.process(any()))
                .thenReturn(Future.succeededFuture(copyRequest));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(downloadExecutors.get(0)).execute(any(), any());

        downloadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
}
