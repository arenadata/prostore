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
package io.arenadata.dtm.query.execution.core.service.schema;

import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.service.schema.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.core.service.schema.impl.LogicalSchemaServiceImpl;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class LogicalSchemaProviderImplTest {

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final LogicalSchemaService logicalSchemaService = mock(LogicalSchemaServiceImpl.class);
    private LogicalSchemaProvider logicalSchemaProvider;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test_datamart");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        logicalSchemaProvider = new LogicalSchemaProviderImpl(logicalSchemaService);
    }

    @Test
    void createSchemaSuccess() {
        Promise promise = Promise.promise();
        final Map<DatamartSchemaKey, Entity> datamartTableMap = new HashMap<>();
        Entity table1 = Entity.builder()
            .schema("test")
            .name("pso")
            .build();

        EntityField attr = EntityField.builder()
            .name("id")
            .type(ColumnType.VARCHAR)
            .primaryOrder(1)
            .shardingOrder(1)
            .build();

        table1.setFields(Collections.singletonList(attr));

        EntityField attr2 = attr.toBuilder()
            .size(10)
            .build();

        Entity table2 = table1.toBuilder()
            .name("doc")
            .fields(Collections.singletonList(attr2))
            .build();

        datamartTableMap.put(new DatamartSchemaKey("test", "doc"), table2);
        datamartTableMap.put(new DatamartSchemaKey("test", "pso"), table1);

        List<Datamart> datamarts = new ArrayList<>();
        Datamart dm = new Datamart();
        dm.setMnemonic("test");
        dm.setEntities(Arrays.asList(table2, table1));
        datamarts.add(dm);
        doAnswer(invocation -> {
            final Handler<AsyncResult<Map<DatamartSchemaKey, Entity>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartTableMap));
            return null;
        }).when(logicalSchemaService).createSchema(any(), any());

        logicalSchemaProvider.getSchema(queryRequest, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(datamarts.get(0).getMnemonic(), ((List<Datamart>) promise.future().result()).get(0).getMnemonic());
        assertEquals(datamarts.get(0).getEntities(), ((List<Datamart>) promise.future().result()).get(0).getEntities());
    }

    @Test
    void createSchemaWithServiceError() {
        Promise promise = Promise.promise();
        doAnswer(invocation -> {
            final Handler<AsyncResult<Map<DatamartSchemaKey, Entity>>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("Ошибка создания схемы!")));
            return null;
        }).when(logicalSchemaService).createSchema(any(), any());

        logicalSchemaProvider.getSchema(queryRequest, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}
