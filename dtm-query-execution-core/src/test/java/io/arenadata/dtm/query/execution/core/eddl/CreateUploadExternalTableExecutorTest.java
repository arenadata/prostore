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
package io.arenadata.dtm.query.execution.core.eddl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.*;
import io.arenadata.dtm.common.plugin.exload.Type;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.service.avro.AvroSchemaGenerator;
import io.arenadata.dtm.query.execution.core.base.service.avro.impl.AvroSchemaGeneratorImpl;
import io.arenadata.dtm.query.execution.core.eddl.dto.CreateUploadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.eddl.service.EddlExecutor;
import io.arenadata.dtm.query.execution.core.eddl.service.upload.CreateUploadExternalTableExecutor;
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
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGeneratorImpl();
    private EddlExecutor createUploadExteranlTableExecutor;
    private CreateUploadExternalTableQuery query;
    private String schema;
    private Entity entity;

    @BeforeEach
    void setUp() {
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
                ExternalTableFormat.AVRO,
                avroSchema.toString(),
                messageSize);

    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createUploadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeDatamartNotExists() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(false));

        createUploadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof DatamartNotExistsException);
    }

    @Test
    void executeEntityExists() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException(entity.getNameWithSchema())));

        createUploadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof EntityAlreadyExistsException);
    }

    @Test
    void executeExistsDatamartError() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.failedFuture(new DtmException("exists datamart error")));

        createUploadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals("exists datamart error", promise.future().cause().getMessage());
    }

    @Test
    void executeCreateEntityError() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new DtmException("create entity error")));

        createUploadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals("create entity error", promise.future().cause().getMessage());
    }
}
