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
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.eddl.DropDownloadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.service.eddl.impl.DropDownloadExternalTableExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropDownloadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private EddlExecutor dropDownloadExternalTableExecutor;
    private DropDownloadExternalTableQuery query;
    private String schema;
    private String table;
    private Entity entity;
    private Entity entityWithWrongType;

    @BeforeEach
    void setUp(){
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        dropDownloadExternalTableExecutor = new DropDownloadExternalTableExecutor(serviceDbFacade);
        schema = "shares";
        table = "accounts";
        query = new DropDownloadExternalTableQuery(schema, table);

        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(20);
        entity = new Entity(table, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);

        entityWithWrongType = new Entity(table, schema, Arrays.asList(f1, f2));
        entityWithWrongType.setEntityType(EntityType.UPLOAD_EXTERNAL_TABLE);
    }

    @Test
    void executeSuccess(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entity));

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture());

        dropDownloadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeTableExistsWithWrongType(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entityWithWrongType));

        dropDownloadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals(String.format("Table [%s] in datamart [%s] doesn't exist!", table, schema), promise.future().cause().getMessage());
    }

    @Test
    void executeTableNotExists(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.failedFuture("entity not exists"));

        dropDownloadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("entity not exists", promise.future().cause().getMessage());
    }

    @Test
    void executeDeleteEntityError(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entity));

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(table)))
                .thenReturn(Future.failedFuture("delete entity error"));

        dropDownloadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("delete entity error", promise.future().cause().getMessage());
    }
}
