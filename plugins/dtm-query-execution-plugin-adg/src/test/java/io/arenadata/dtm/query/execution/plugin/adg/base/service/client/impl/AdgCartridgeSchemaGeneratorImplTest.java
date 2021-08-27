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
package io.arenadata.dtm.query.execution.plugin.adg.base.service.client.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.base.dto.AdgTables;
import io.arenadata.dtm.query.execution.plugin.adg.ddl.factory.AdgCreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema.AdgSpace;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema.Space;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeSchemaGenerator;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

class AdgCartridgeSchemaGeneratorImplTest {

    private ObjectMapper mapper;
    private DdlRequest ddlRequest;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper(new YAMLFactory());
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic("test");
        List<EntityField> fields = Collections.singletonList(new EntityField(0, "test_field", "varchar(1)", false, ""));
        Entity entity = new Entity("test_schema.test_table", fields);

        ddlRequest = new DdlRequest(UUID.randomUUID(),
                "test",
                queryRequest.getDatamartMnemonic(),
                entity,
                SqlKind.CREATE_TABLE);
    }

    @Test
    void generateWithEmptySpaces() throws JsonProcessingException {
        Promise<OperationYaml> promise = Promise.promise();
        AdgSpace adgSpace = new AdgSpace("test", new Space());
        AdgTables<AdgSpace> adgCreateTableQueries = new AdgTables<>(adgSpace, adgSpace, adgSpace);
        CreateTableQueriesFactory<AdgTables<AdgSpace>> createTableQueriesFactory = mock(AdgCreateTableQueriesFactory.class);
        Mockito.when(createTableQueriesFactory.create(any(), any())).thenReturn(adgCreateTableQueries);
        AdgCartridgeSchemaGenerator cartridgeSchemaGenerator = new AdgCartridgeSchemaGeneratorImpl(createTableQueriesFactory);
        cartridgeSchemaGenerator.generate(ddlRequest, mapper.readValue("{}", OperationYaml.class))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());

                    } else {
                        promise.fail(ar.cause());
                    }
                });
        assertTrue(promise.future().succeeded());
    }
}
