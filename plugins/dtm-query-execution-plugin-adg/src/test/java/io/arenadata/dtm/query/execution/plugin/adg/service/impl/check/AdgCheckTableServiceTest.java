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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl.check;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgCreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.*;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.AdgCartridgeClientImpl;
import io.arenadata.dtm.query.execution.plugin.adg.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalMatchers;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;
import static io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgTableEntitiesFactory.SEC_INDEX_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdgCheckTableServiceTest {
    private static final String TEST_COLUMN_NAME = "test_column";
    private static final String NOT_TABLE_EXIST = "not_exist_table";
    private static final String ENV = "env";
    private Entity entity;
    private CheckContext checkContext;
    private CheckTableService adgCheckTableService;

    @BeforeEach
    void setUp() {

        AdgCartridgeClient adgClient = mock(AdgCartridgeClientImpl.class);
        entity = TestUtils.getEntity();
        int fieldsCount = entity.getFields().size();
        entity.getFields().add(EntityField.builder()
                .name(TEST_COLUMN_NAME)
                .ordinalPosition(fieldsCount + 1)
                .type(ColumnType.BIGINT)
                .nullable(true)
                .build());

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setEnvName(ENV);
        queryRequest.setDatamartMnemonic(entity.getSchema());
        checkContext = new CheckContext(null, new DatamartRequest(queryRequest), entity);

        Map<String, Space> spaces = TestUtils.getSpaces(entity);
        when(adgClient.getSpaceDescriptions(eq(spaces.keySet())))
                .thenReturn(Future.succeededFuture(spaces));
        when(adgClient.getSpaceDescriptions(AdditionalMatchers.not(eq(spaces.keySet()))))
                .thenReturn(Future.failedFuture(String.format(CheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                        NOT_TABLE_EXIST + ACTUAL_POSTFIX)));

        adgCheckTableService = new AdgCheckTableService(adgClient,
                new AdgCreateTableQueriesFactory(new AdgTableEntitiesFactory(new TarantoolDatabaseProperties())));
    }

    @Test
    void testSuccess() {
        assertTrue(adgCheckTableService.check(checkContext).succeeded());
    }

    @Test
    void testTableNotExist() {
        entity.setName("not_exist_table");
        assertThat(adgCheckTableService.check(checkContext).cause().getMessage(),
                containsString(String.format(CheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                        NOT_TABLE_EXIST + ACTUAL_POSTFIX)));
    }

    @Test
    void testColumnNotExist() {
        entity.getFields().add(EntityField.builder()
                .name("not_exist_column")
                .size(1)
                .type(ColumnType.VARCHAR)
                .build());
        String expectedError = String.format(AdgCheckTableService.COLUMN_NOT_EXIST_ERROR_TEMPLATE,
                "not_exist_column");
        assertThat(adgCheckTableService.check(checkContext).cause().getMessage(),
                containsString(expectedError));
    }

    @Test
    void testDataType() {
        String expectedError = String.format(CheckTableService.FIELD_ERROR_TEMPLATE,
                CheckTableService.DATA_TYPE, "string", "integer");
        testColumns(field -> field.setType(ColumnType.VARCHAR), expectedError);

    }

    private void testColumns(Consumer<EntityField> consumer,
                             String expectedError) {
        EntityField testColumn = entity.getFields().stream()
                .filter(field -> TEST_COLUMN_NAME.equals(field.getName()))
                .findAny()
                .orElseThrow(RuntimeException::new);
        consumer.accept(testColumn);
        assertThat(adgCheckTableService.check(checkContext).cause().getMessage(),
                containsString(expectedError));
    }
}
