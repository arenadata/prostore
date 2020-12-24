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
package io.arenadata.dtm.query.execution.core.service.eddl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.common.plugin.exload.Type;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.eddl.CreateUploadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.service.avro.AvroSchemaGenerator;
import io.arenadata.dtm.query.execution.core.service.avro.AvroSchemaGeneratorImpl;
import io.arenadata.dtm.query.execution.core.service.eddl.impl.CreateUploadExternalTableExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateUploadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGeneratorImpl();
    private EddlExecutor createUploadExteranlTableExecutor;
    private CreateUploadExternalTableQuery query;
    private String schema;
    private Entity entity;

    @BeforeEach
    void setUp(){
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        createUploadExteranlTableExecutor = new CreateUploadExternalTableExecutor(serviceDbFacade);

        schema = "shares";
        String table = "accounts";
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(20);
        entity = new Entity(table, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.UPLOAD_EXTERNAL_TABLE);
        Schema avroSchema = avroSchemaGenerator.generateTableSchema(entity, false);
        int messageSize = 10;
        String locationPath = "kafka://localhost:2181/KAFKA_TOPIC";
        query = new CreateUploadExternalTableQuery(schema,
                table,
                entity,
                Type.KAFKA_TOPIC,
                locationPath,
                Format.AVRO,
                avroSchema.toString(),
                messageSize);

    }

    @Test
    void executeSuccess(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(false));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createUploadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeDatamartNotExists(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(false));

        createUploadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals(String.format("Datamart [%s] not exists!", schema), promise.future().cause().getMessage());
    }

    @Test
    void executeTableExists(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(true));

        createUploadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals(String.format("Table [%s] is already exists in datamart [%s]!", entity.getName(), schema), promise.future().cause().getMessage());
    }

    @Test
    void executeExistsDatamartError(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.failedFuture("exists datamart error"));

        createUploadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("exists datamart error", promise.future().cause().getMessage());
    }

    @Test
    void executeExistsEntityError(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.failedFuture("exists entity error"));

        createUploadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("exists entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeCreateEntityError(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(false));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture("create entity error"));

        createUploadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("create entity error", promise.future().cause().getMessage());
    }
}
