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
package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetadataSqlFactoryImplTest {
    private MetadataSqlFactory metadataSqlFactory;
    private static final String EXPECTED_DROP_SCRIPTS = "DROP TABLE IF EXISTS test.test_ts3222_actual;" +
            " DROP TABLE IF EXISTS test.test_ts3222_history; " +
            "DROP TABLE IF EXISTS test.test_ts3222_staging; ";
    private static final String INDEXES_QUERY_EXPECTED = "CREATE INDEX test_actual_sys_from_idx ON shares.test_actual (sys_from);" +
            " CREATE INDEX test_history_sys_to_idx ON shares.test_history (sys_to, sys_op)";

    @BeforeEach
    void setUp() {
        metadataSqlFactory = new MetadataSqlFactoryImpl();
    }

    @Test
    void createDropTableScript() {
        String tableScript = metadataSqlFactory.createDropTableScript(getClassTable());
        assertEquals(EXPECTED_DROP_SCRIPTS, tableScript);
    }

    @Test
    void createIndexesTest() {
        String indexedQuery = metadataSqlFactory.createSecondaryIndexSqlQuery("shares", "test");
        assertEquals(INDEXES_QUERY_EXPECTED, indexedQuery);
    }

    private Entity getClassTable() {
        return new Entity("test.test_ts3222", Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "name", ColumnType.VARCHAR.name(), true, null, null, null),
                new EntityField(2, "dt", ColumnType.TIMESTAMP.name(), true, null, 2, null)
        ));
    }
}
