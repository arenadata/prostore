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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.check;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityTypeUtil;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.AdbTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.CreateEntityUtils;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdbCheckTableServiceTest {

    private static Map<String, List<Map<String, Object>>> sysColumns;
    private CheckTableService adbCheckTableService;
    private final List<String> tablePostFixes = Arrays.asList(
            AdbTables.ACTUAL_TABLE_POSTFIX,
            AdbTables.HISTORY_TABLE_POSTFIX,
            AdbTables.STAGING_TABLE_POSTFIX);
    private Entity entity;
    private CheckContext checkContext;
    private static final String TEST_COLUMN_NAME = "test_column";

    @BeforeAll
    static void init() {
        Map<String, Object> sysFromAttr = new HashMap<>();
        sysFromAttr.put(AdbCheckTableService.CONSTRAINT_TYPE, null);
        sysFromAttr.put(AdbCheckTableService.DATETIME_PRECISION, null);
        sysFromAttr.put(AdbCheckTableService.ORDINAL_POSITION, null);
        sysFromAttr.put(AdbCheckTableService.COLUMN_NAME, AdbTableEntitiesFactory.SYS_FROM_ATTR);
        sysFromAttr.put(AdbCheckTableService.DATA_TYPE, "int8");
        sysFromAttr.put(AdbCheckTableService.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> sysToAttr = new HashMap<>();
        sysToAttr.put(AdbCheckTableService.CONSTRAINT_TYPE, null);
        sysToAttr.put(AdbCheckTableService.DATETIME_PRECISION, null);
        sysToAttr.put(AdbCheckTableService.ORDINAL_POSITION, null);
        sysToAttr.put(AdbCheckTableService.COLUMN_NAME, AdbTableEntitiesFactory.SYS_TO_ATTR);
        sysToAttr.put(AdbCheckTableService.DATA_TYPE, "int8");
        sysToAttr.put(AdbCheckTableService.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> sysOpAttr = new HashMap<>();
        sysOpAttr.put(AdbCheckTableService.CONSTRAINT_TYPE, null);
        sysOpAttr.put(AdbCheckTableService.DATETIME_PRECISION, null);
        sysOpAttr.put(AdbCheckTableService.ORDINAL_POSITION, null);
        sysOpAttr.put(AdbCheckTableService.COLUMN_NAME, AdbTableEntitiesFactory.SYS_OP_ATTR);
        sysOpAttr.put(AdbCheckTableService.DATA_TYPE, "int4");
        sysOpAttr.put(AdbCheckTableService.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> req_id = new HashMap<>();
        req_id.put(AdbCheckTableService.CONSTRAINT_TYPE, null);
        req_id.put(AdbCheckTableService.DATETIME_PRECISION, null);
        req_id.put(AdbCheckTableService.ORDINAL_POSITION, null);
        req_id.put(AdbCheckTableService.COLUMN_NAME, AdbTableEntitiesFactory.REQ_ID_ATTR);
        req_id.put(AdbCheckTableService.DATA_TYPE, "varchar(36)");
        req_id.put(AdbCheckTableService.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> PKSysFromAttr = new HashMap<>();
        PKSysFromAttr.put(AdbCheckTableService.CONSTRAINT_TYPE, null);
        PKSysFromAttr.put(AdbCheckTableService.DATETIME_PRECISION, null);
        PKSysFromAttr.put(AdbCheckTableService.ORDINAL_POSITION, 3);
        PKSysFromAttr.put(AdbCheckTableService.COLUMN_NAME, AdbTableEntitiesFactory.SYS_FROM_ATTR);
        PKSysFromAttr.put(AdbCheckTableService.DATA_TYPE, "int8");
        PKSysFromAttr.put(AdbCheckTableService.CHARACTER_MAXIMUM_LENGTH, null);

        sysColumns = new HashMap<>();
        sysColumns.put(AdbTables.ACTUAL_TABLE_POSTFIX, Arrays.asList(PKSysFromAttr, sysToAttr, sysOpAttr));
        sysColumns.put(AdbTables.HISTORY_TABLE_POSTFIX, Arrays.asList(PKSysFromAttr, sysToAttr, sysOpAttr));
        sysColumns.put(AdbTables.STAGING_TABLE_POSTFIX, Arrays.asList(sysFromAttr, sysToAttr, sysOpAttr, req_id));

    }

    @BeforeEach
    void setUp() {
        DatabaseExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
        entity = CreateEntityUtils.getEntity();

        int fieldsCount = entity.getFields().size();
        entity.getFields().add(EntityField.builder()
                .name(TEST_COLUMN_NAME)
                .ordinalPosition(fieldsCount + 1)
                .type(ColumnType.BIGINT)
                .nullable(true)
                .build());

        List<Map<String, Object>> resultSet = entity.getFields().stream()
                .map(this::fieldToMapTransform)
                .collect(Collectors.toList());

        checkContext = new CheckContext(null, new DatamartRequest(new QueryRequest()), entity);

        tablePostFixes.forEach(postFix -> when(adbQueryExecutor.execute(argThat(getPredicate(postFix)::test)))
                .thenReturn(Future.succeededFuture(getResultSet(resultSet, postFix))));

        List<String> queries = tablePostFixes.stream()
                .map(postFix -> String.format(AdbCheckTableService.QUERY_PATTERN_WITH_CONDITION,
                        entity.getSchema(), String.format("%s_%s", entity.getName(), postFix)))
                .collect(Collectors.toList());
        when(adbQueryExecutor.execute(argThat(arg -> queries.stream().noneMatch(arg::equals))))
                .thenReturn(Future.succeededFuture(Collections.emptyList()));
        adbCheckTableService = new AdbCheckTableService(adbQueryExecutor, new AdbTableEntitiesFactory());
    }

    @Test
    void testSuccess() {
        assertTrue(adbCheckTableService.check(checkContext).succeeded());
    }

    @Test
    void testTableNotExist() {
        entity.setName("not_exist_table");
        Future<Void> result = adbCheckTableService.check(checkContext);
        tablePostFixes.forEach(postFix ->
                assertThat(result.cause().getMessage(),
                        containsString(String.format(AdbCheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                                String.format("not_exist_table_%s", postFix)))));
    }

    @Test
    void testColumnNotExist() {
        entity.getFields().add(EntityField.builder()
                .name("not_exist_column")
                .size(1)
                .type(ColumnType.VARCHAR)
                .build());
        String expectedError = String.format(AdbCheckTableService.COLUMN_NOT_EXIST_ERROR_TEMPLATE,
                "not_exist_column");
        assertThat(adbCheckTableService.check(checkContext).cause().getMessage(),
                containsString(expectedError));
    }

    @Test
    void testDataType() {
        String expectedError = String.format(AdbCheckTableService.FIELD_ERROR_TEMPLATE,
                AdbCheckTableService.DATA_TYPE, ColumnType.TIME.name().toLowerCase(), "int8");
        testColumns(field -> field.setType(ColumnType.TIME), expectedError);

    }

    @Test
    void testPrimaryKey() {
        String expectedError = String.format(AdbCheckTableService.PRIMARY_KEY_ERROR_TEMPLATE,
                "id, pk2, test_column", "id, pk2");
        testColumns(field -> field.setPrimaryOrder(4), expectedError);

    }

    private void testColumns(Consumer<EntityField> consumer,
                             String expectedError) {
        EntityField testColumn = entity.getFields().stream()
                .filter(field -> TEST_COLUMN_NAME.equals(field.getName()))
                .findAny()
                .orElseThrow(RuntimeException::new);
        consumer.accept(testColumn);
        assertThat(adbCheckTableService.check(checkContext).cause().getMessage(),
                containsString(expectedError));
    }

    private Predicate<String> getPredicate(String postFix) {
        String query = String.format(AdbCheckTableService.QUERY_PATTERN_WITH_CONDITION,
                entity.getSchema(), String.format("%s_%s", entity.getName(), postFix));
        return query::equals;
    }

    private List<Map<String, Object>> getResultSet(List<Map<String, Object>> baseResultSet,
                                                   String postFix) {
        List<Map<String, Object>> resultSet = new ArrayList<>(baseResultSet);
        resultSet.addAll(sysColumns.get(postFix));
        return resultSet;
    }

    private Map<String, Object> fieldToMapTransform(EntityField field) {
        HashMap<String, Object> result = new HashMap<>();
        result.put(AdbCheckTableService.CONSTRAINT_TYPE, Optional.ofNullable(field.getPrimaryOrder())
                .map(o -> AdbCheckTableService.PRIMARY_KEY_CS_TYPE).orElse(null));
        result.put(AdbCheckTableService.DATETIME_PRECISION, field.getAccuracy());
        result.put(AdbCheckTableService.ORDINAL_POSITION, field.getPrimaryOrder());
        result.put(AdbCheckTableService.COLUMN_NAME, field.getName());
        result.put(AdbCheckTableService.DATA_TYPE, EntityTypeUtil.pgFromDtmType(field));
        result.put(AdbCheckTableService.CHARACTER_MAXIMUM_LENGTH, field.getSize());
        return result;
    }
}
