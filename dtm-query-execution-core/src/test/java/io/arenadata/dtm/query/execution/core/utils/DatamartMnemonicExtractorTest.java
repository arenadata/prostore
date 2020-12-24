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
package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.DeltaInformationExtractorImpl;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

class DatamartMnemonicExtractorTest {
    public static final String EXPECTED_DATAMART = "test";
    private final CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private final DatamartMnemonicExtractor extractor = new DatamartMnemonicExtractor(
            new DeltaInformationExtractorImpl(new CoreDtmSettings(ZoneId.of("UTC"))));

    @Test
    void extractFromSelect() {
        val sqlNode = definitionService.processingQuery("select * from test.tbl1");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromSelectWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            SqlNode sqlNode = definitionService.processingQuery("select * from tbl1");
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromSelectSnapshot() {
        val sqlNode = definitionService.processingQuery("select * from test.tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
        assertTrue(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql().contains("FOR SYSTEM_TIME AS OF "));
    }

    @Test
    void extractFromSelectSnapshotWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            val sqlNode = definitionService.processingQuery("select * from tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t");
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromInnerSelect() {
        val sqlNode = definitionService.processingQuery("select * from (select id from test.tbl1) AS t");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromInnerSelectSnapshot() {
        val sqlNode = definitionService.processingQuery("select * from (select * from test.tbl1" +
                " FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14') AS t");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromJoin() {
        val sqlNode = definitionService.processingQuery("select * from test.tbl1 " +
                "JOIN test.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromCreateTable() {
        val sqlNode = definitionService.processingQuery("CREATE TABLE test.table_name " +
                "(col1 datatype1, col2 datatype2, PRIMARY KEY (col1, col2) )" +
                " DISTRIBUTED BY (col1, col2)");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromCreateTableWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            val sqlNode = definitionService.processingQuery(
                    "CREATE TABLE table_name (col1 datatype1, col2 datatype2, PRIMARY KEY (col1, col2) )" +
                            " DISTRIBUTED BY (col1, col2)"
            );
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromDropTable() {
        val sqlNode = definitionService.processingQuery("DROP TABLE test.table_name");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromDropTableWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            val sqlNode = definitionService.processingQuery("DROP TABLE table_name");
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromCreateView() {
        val sqlNode = definitionService.processingQuery("CREATE VIEW test.view1 as select * from test2.tbl1" +
                " JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromCreateOrReplaceView() {
        val sqlNode = definitionService.processingQuery("CREATE OR REPLACE VIEW test.view1 as select * from test2.tbl1" +
                " JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromCreateViewWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            val sqlNode = definitionService.processingQuery("CREATE VIEW view1 as select * from test2.tbl1 " +
                    "JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromInsert() {
        val sqlNode = definitionService.processingQuery("INSERT INTO test.PSO SELECT * FROM test.PSO");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }
}
