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
package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.impl.TargetDatabaseDefinitionServiceImpl;
import io.arenadata.dtm.query.execution.core.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TargetDatabaseDefinitionServiceImplTest {

    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private TargetDatabaseDefinitionService targetDatabaseDefinitionService;
    private Set<SourceType> defaultSourceTypes = SourceType.pluginsSourceTypes();

    @BeforeEach
    void setUp() {
        targetDatabaseDefinitionService = new TargetDatabaseDefinitionServiceImpl(dataSourcePluginService,
                entityDao, new CoreDtmSettings(ZoneId.of("UTC")));
    }

    @Test
    void getTargetSourceWithCalcCosts() {
        Promise promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();
        final Integer adbQueryCost = 1;
        final Integer adgQueryCost = 0;

        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        when(dataSourcePluginService.getSourceTypes()).thenReturn(defaultSourceTypes);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(adbQueryCost));
            return null;
        }).when(dataSourcePluginService).calcQueryCost(eq(SourceType.ADB), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(adgQueryCost));
            return null;
        }).when(dataSourcePluginService).calcQueryCost(eq(SourceType.ADG), any(), any());

        targetDatabaseDefinitionService.getTargetSource(sourceRequest, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result().getSourceType());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertEquals(SourceType.ADG, promise.future().result());
    }

    @Test
    void getTargetSourceWithSettedSourceType() {
        Promise promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();

        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setSourceType(SourceType.ADB);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        when(dataSourcePluginService.getSourceTypes()).thenReturn(defaultSourceTypes);

        targetDatabaseDefinitionService.getTargetSource(sourceRequest, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result().getSourceType());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertEquals(SourceType.ADB, promise.future().result());
    }

    @Test
    void getTargetSourceWithSingleTableSourceType() {
        Promise promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();
        schema.get(1).getEntities().get(0)
                .setDestination(new HashSet<>(Collections.singletonList(SourceType.ADG)));

        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        when(dataSourcePluginService.getSourceTypes()).thenReturn(defaultSourceTypes);

        targetDatabaseDefinitionService.getTargetSource(sourceRequest, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result().getSourceType());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertEquals(SourceType.ADG, promise.future().result());
    }

    @Test
    void getTargetSourceWithNonExistTableSourceType() {
        Promise promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();
        schema.get(0).getEntities().get(0)
                .setDestination(new HashSet<>(Collections.singletonList(SourceType.ADB)));
        schema.get(1).getEntities().get(0)
                .setDestination(new HashSet<>(Collections.singletonList(SourceType.ADQM)));

        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        when(dataSourcePluginService.getSourceTypes()).thenReturn(defaultSourceTypes);

        targetDatabaseDefinitionService.getTargetSource(sourceRequest, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result().getSourceType());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void getTargetSourceWithNonEqualSettedSourceType() {
        Promise promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();
        schema.get(1).getEntities().get(0)
                .setDestination(new HashSet<>(Collections.singletonList(SourceType.ADQM)));

        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setSourceType(SourceType.ADG);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        when(dataSourcePluginService.getSourceTypes()).thenReturn(defaultSourceTypes);

        targetDatabaseDefinitionService.getTargetSource(sourceRequest, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result().getSourceType());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void getTargetSourceWithEqualCalcCosts() {
        Promise promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();
        schema.get(1).getEntities().get(0)
                .setDestination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADQM)));

        final Integer adbQueryCost = 0;
        final Integer adgQueryCost = 0;
        final Integer adqmQueryCost = 0;

        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        when(dataSourcePluginService.getSourceTypes()).thenReturn(defaultSourceTypes);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(adbQueryCost));
            return null;
        }).when(dataSourcePluginService).calcQueryCost(eq(SourceType.ADB), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(adgQueryCost));
            return null;
        }).when(dataSourcePluginService).calcQueryCost(eq(SourceType.ADG), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(adqmQueryCost));
            return null;
        }).when(dataSourcePluginService).calcQueryCost(eq(SourceType.ADQM), any(), any());

        targetDatabaseDefinitionService.getTargetSource(sourceRequest, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result().getSourceType());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertEquals(SourceType.ADB, promise.future().result());
    }

    @Test
    void getTargetSourceWithCalcCostsError() {
        Promise promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();
        final Integer adgQueryCost = 0;

        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        when(dataSourcePluginService.getSourceTypes()).thenReturn(defaultSourceTypes);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(dataSourcePluginService).calcQueryCost(eq(SourceType.ADB), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(adgQueryCost));
            return null;
        }).when(dataSourcePluginService).calcQueryCost(eq(SourceType.ADG), any(), any());

        targetDatabaseDefinitionService.getTargetSource(sourceRequest, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result().getSourceType());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    private List<Datamart> createLogicalSchema() {
        List<Entity> entities1 = new ArrayList<>();
        entities1.addAll(Arrays.asList(Entity.builder()
                        .name("table_1")
                        .schema("dtm_1")
                        .entityType(EntityType.TABLE)
                        .destination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADQM)))
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("id")
                                        .nullable(false)
                                        .type(ColumnType.BIGINT)
                                        .ordinalPosition(0)
                                        .primaryOrder(0)
                                        .build(),
                                EntityField.builder()
                                        .name("name")
                                        .nullable(true)
                                        .type(ColumnType.VARCHAR)
                                        .ordinalPosition(1)
                                        .size(100)
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("table_2")
                        .schema("dtm_1")
                        .entityType(EntityType.TABLE)
                        .destination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADQM)))
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("id")
                                        .nullable(false)
                                        .type(ColumnType.BIGINT)
                                        .ordinalPosition(0)
                                        .primaryOrder(0)
                                        .build(),
                                EntityField.builder()
                                        .name("name")
                                        .nullable(true)
                                        .type(ColumnType.VARCHAR)
                                        .ordinalPosition(1)
                                        .size(100)
                                        .build()))
                        .build()
        ));
        List<Entity> entities2 = new ArrayList<>();
        entities2.add(Entity.builder()
                .name("table_3")
                .schema("dtm_2")
                .entityType(EntityType.TABLE)
                .destination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG)))
                .fields(Arrays.asList(EntityField.builder()
                                .name("id")
                                .nullable(false)
                                .type(ColumnType.BIGINT)
                                .ordinalPosition(0)
                                .primaryOrder(0)
                                .build(),
                        EntityField.builder()
                                .name("name")
                                .nullable(true)
                                .type(ColumnType.VARCHAR)
                                .ordinalPosition(1)
                                .size(100)
                                .build()
                ))
                .build());
        Datamart d1 = new Datamart("dtm_1", true, entities1);
        Datamart d2 = new Datamart("dtm_2", true, entities2);
        return new ArrayList<>(Arrays.asList(d1, d2));
    }
}
