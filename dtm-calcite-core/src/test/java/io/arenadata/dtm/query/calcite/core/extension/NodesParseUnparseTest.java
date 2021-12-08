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
package io.arenadata.dtm.query.calcite.core.extension;

import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NodesParseUnparseTest {
    private static final SqlDialect SQL_DIALECT = new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT);

    @Test
    void shouldParseUnparseSqlAlterView() throws SqlParseException {
        // arrange
        val originalQuery = "ALTER VIEW db.view_name AS SELECT * FROM tbl DATASOURCE_TYPE='ADB'";
        val parser = getParser(originalQuery);

        // act
        val sqlNode = parser.parseQuery();

        // assert
        val unparsedQuery = sqlNode.toSqlString(SQL_DIALECT).getSql();
        assertThat(unparsedQuery).isEqualToIgnoringWhitespace(originalQuery);
    }

    @Test
    void shouldParseUnparseSqlCreateMaterializedView() throws SqlParseException {
        // arrange
        val originalQuery = "CREATE MATERIALIZED VIEW db.materialized_view_name ( " +
                "  column_name_1 INTEGER NOT NULL, " +
                "  column_name_2 BOOLEAN DEFAULT (TRUE), " +
                "  column_name_3 BIGINT, " +
                "  PRIMARY KEY (column_name_1) " +
                ") DISTRIBUTED BY (column_name_1) " +
                "DATASOURCE_TYPE (adb,adg) " +
                "AS SELECT * FROM tbl " +
                "DATASOURCE_TYPE = 'ADQM' " +
                "LOGICAL_ONLY";
        val parser = getParser(originalQuery);

        // act
        val sqlNode = parser.parseQuery();

        // assert
        val unparsedQuery = sqlNode.toSqlString(SQL_DIALECT).getSql();
        assertThat(unparsedQuery).isEqualToIgnoringWhitespace(originalQuery);
    }

    @Test
    void shouldParseUnparseSqlCreateTable() throws SqlParseException {
        // arrange
        val originalQuery = "CREATE TABLE db.table_name ( " +
                "  column_name_1 INTEGER NOT NULL, " +
                "  column_name_2 BOOLEAN DEFAULT (TRUE), " +
                "  column_name_3 BIGINT, " +
                "  PRIMARY KEY (column_name_1) " +
                ") DISTRIBUTED BY (column_name_1) " +
                "DATASOURCE_TYPE (adb,adqm) " +
                "LOGICAL_ONLY";
        val parser = getParser(originalQuery);

        // act
        val sqlNode = parser.parseQuery();

        // assert
        val unparsedQuery = sqlNode.toSqlString(SQL_DIALECT).getSql();
        assertThat(unparsedQuery).isEqualToIgnoringWhitespace(originalQuery);
    }

    @Test
    void shouldParseUnparseSqlCreateView() throws SqlParseException {
        // arrange
        val originalQuery = "CREATE OR REPLACE VIEW db.view_name " +
                "AS SELECT * FROM tbl " +
                "DATASOURCE_TYPE = 'ADQM' ";
        val parser = getParser(originalQuery);

        // act
        val sqlNode = parser.parseQuery();

        // assert
        val unparsedQuery = sqlNode.toSqlString(SQL_DIALECT).getSql();
        assertThat(unparsedQuery).isEqualToIgnoringWhitespace(originalQuery);
    }

    @Test
    void shouldParseUnparseSqlDropMaterializedViewView() throws SqlParseException {
        // arrange
        val originalQuery = "DROP MATERIALIZED VIEW IF EXISTS db_name.materialized_view_name " +
                "DATASOURCE_TYPE = 'ADB' " +
                "LOGICAL_ONLY";
        val parser = getParser(originalQuery);

        // act
        val sqlNode = parser.parseQuery();

        // assert
        val unparsedQuery = sqlNode.toSqlString(SQL_DIALECT).getSql();
        assertThat(unparsedQuery).isEqualToIgnoringWhitespace(originalQuery);
    }

    @Test
    void shouldParseUnparseSqlDropTable() throws SqlParseException {
        // arrange
        val originalQuery = "DROP TABLE IF EXISTS db_name.table_name " +
                "DATASOURCE_TYPE = 'adb' " +
                "LOGICAL_ONLY";
        val parser = getParser(originalQuery);

        // act
        val sqlNode = parser.parseQuery();

        // assert
        val unparsedQuery = sqlNode.toSqlString(SQL_DIALECT).getSql();
        assertThat(unparsedQuery).isEqualToIgnoringWhitespace(originalQuery);
    }

    @Test
    void shouldParseUnparseSqlDropView() throws SqlParseException {
        // arrange
        val originalQuery = "DROP VIEW db_name.view_name";
        val parser = getParser(originalQuery);

        // act
        val sqlNode = parser.parseQuery();

        // assert
        val unparsedQuery = sqlNode.toSqlString(SQL_DIALECT).getSql();
        assertThat(unparsedQuery).isEqualToIgnoringWhitespace(originalQuery);
    }

    @Test
    void shouldParseUnparseOrderByNode() throws SqlParseException {
        // arrange
        val originalQuery = "SELECT * FROM tbl ORDER BY id LIMIT 1 OFFSET 1 DATASOURCE_TYPE='adb'";
        val parser = getParser(originalQuery);

        // act
        val sqlNode = parser.parseQuery();

        // assert
        val unparsedQuery = sqlNode.toSqlString(SQL_DIALECT).getSql();
        assertThat(unparsedQuery).isEqualToIgnoringWhitespace(originalQuery);
    }

    @Test
    void shouldParseUnparseSelectExtNode() throws SqlParseException {
        // arrange
        val originalQuery = "SELECT * FROM tbl DATASOURCE_TYPE='adb'";
        val parser = getParser(originalQuery);

        // act
        val sqlNode = parser.parseQuery();

        // assert
        val unparsedQuery = sqlNode.toSqlString(SQL_DIALECT).getSql();
        assertThat(unparsedQuery).isEqualToIgnoringWhitespace(originalQuery);
    }

    private SqlParser getParser(String sql) {
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(new CalciteCoreConfiguration().eddlParserImplFactory())
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        return SqlParser.create(sql, config);
    }
}
