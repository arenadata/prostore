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
package io.arenadata.dtm.query.execution.core.edml;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlAction;
import io.arenadata.dtm.query.execution.core.edml.service.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.edml.service.EdmlService;
import io.arenadata.dtm.query.execution.core.edml.mppr.service.impl.DownloadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.edml.service.impl.EdmlServiceImpl;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.UploadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.utils.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EdmlServiceImplTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final List<EdmlExecutor> edmlExecutors = Arrays.asList(mock(DownloadExternalTableExecutor.class),
            mock(UploadExternalTableExecutor.class));
    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private EdmlService<QueryResult> edmlService;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
    }

    @Test
    void executeDownloadExtTableSuccess() {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDbFacade, edmlExecutors);
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table")).thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso")).thenReturn(Future.succeededFuture(sourceEntity));

        when(edmlExecutors.get(0).execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        edmlService.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(context.getSourceEntity(), sourceEntity);
        assertEquals(context.getDestinationEntity(), destinationEntity);
    }

    @Test
    void executeUploadExtTableSuccess() {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDbFacade, edmlExecutors);
        Promise<QueryResult> promise = Promise.promise();
        queryRequest.setSql("INSERT INTO test.pso SELECT id, name FROM test.upload_table");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("pso")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("upload_table")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "pso"))
                .thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "upload_table"))
                .thenReturn(Future.succeededFuture(sourceEntity));

        when(edmlExecutors.get(1).execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        edmlService.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(context.getSourceEntity(), sourceEntity);
        assertEquals(context.getDestinationEntity(), destinationEntity);
    }

    @Test
    void executeDownloadExtTableAsSource() {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDbFacade, edmlExecutors);
        Promise<QueryResult> promise = Promise.promise();
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table"))
                .thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso"))
                .thenReturn(Future.succeededFuture(sourceEntity));

        edmlService.execute(context)
                .onComplete(promise);
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeUploadExtTableAsDestination() {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDbFacade, edmlExecutors);
        Promise<QueryResult> promise = Promise.promise();
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table"))
                .thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso"))
                .thenReturn(Future.succeededFuture(sourceEntity));

        edmlService.execute(context)
                .onComplete(promise);
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeUploadExtTableToMaterializedView() {
        // arrange
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDbFacade, edmlExecutors);
        Promise<QueryResult> promise = Promise.promise();
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.MATERIALIZED_VIEW)
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table"))
                .thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso"))
                .thenReturn(Future.succeededFuture(sourceEntity));

        // act
        edmlService.execute(context)
                .onComplete(promise);

        // assert
        assertTrue(promise.future().failed());
        TestUtils.assertException(DtmException.class, "MPPR/MPPW operation doesn't support materialized views", promise.future().cause());
    }
}
