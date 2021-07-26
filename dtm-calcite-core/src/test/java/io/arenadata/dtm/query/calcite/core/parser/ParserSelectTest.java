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
package io.arenadata.dtm.query.calcite.core.parser;

import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

public class ParserSelectTest {
    
    @Test
    void shouldParseWhenLimitAndOffset() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 a LIMIT 1 OFFSET 2";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(LimitableSqlOrderBy.class, sqlNode.getClass());
        LimitableSqlOrderBy limitableSqlOrderBy = (LimitableSqlOrderBy) sqlNode;
        assertNotNull(limitableSqlOrderBy.fetch);
        assertEquals(BigDecimal.valueOf(1), ((SqlNumericLiteral)limitableSqlOrderBy.fetch).getValue());

        assertNotNull(limitableSqlOrderBy.offset);
        assertEquals(BigDecimal.valueOf(2), ((SqlNumericLiteral)limitableSqlOrderBy.offset).getValue());
    }

    @Test
    void shouldParseWhenFetchAndOffset() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 FETCH NEXT 1 ROWS ONLY OFFSET 2";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(LimitableSqlOrderBy.class, sqlNode.getClass());
        LimitableSqlOrderBy limitableSqlOrderBy = (LimitableSqlOrderBy) sqlNode;
        assertNotNull(limitableSqlOrderBy.fetch);
        assertEquals(BigDecimal.valueOf(1), ((SqlNumericLiteral)limitableSqlOrderBy.fetch).getValue());

        assertNotNull(limitableSqlOrderBy.offset);
        assertEquals(BigDecimal.valueOf(2), ((SqlNumericLiteral)limitableSqlOrderBy.offset).getValue());
    }

    @Test
    void shouldParseWhenLimitAndOffsetDynamic() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 a LIMIT ? OFFSET ?";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(LimitableSqlOrderBy.class, sqlNode.getClass());
        LimitableSqlOrderBy limitableSqlOrderBy = (LimitableSqlOrderBy) sqlNode;
        assertNotNull(limitableSqlOrderBy.fetch);
        assertSame(SqlKind.DYNAMIC_PARAM, limitableSqlOrderBy.fetch.getKind());

        assertNotNull(limitableSqlOrderBy.offset);
        assertSame(SqlKind.DYNAMIC_PARAM, limitableSqlOrderBy.fetch.getKind());
    }

    @Test
    void shouldParseWhenFetchAndOffsetDynamic() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 FETCH NEXT ? ROWS ONLY OFFSET ?";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(LimitableSqlOrderBy.class, sqlNode.getClass());
        LimitableSqlOrderBy limitableSqlOrderBy = (LimitableSqlOrderBy) sqlNode;
        assertNotNull(limitableSqlOrderBy.fetch);
        assertSame(SqlKind.DYNAMIC_PARAM, limitableSqlOrderBy.fetch.getKind());

        assertNotNull(limitableSqlOrderBy.offset);
        assertSame(SqlKind.DYNAMIC_PARAM, limitableSqlOrderBy.fetch.getKind());
    }

    @Test
    void shouldFailWhenOffsetAndFetchLast() {
        // arrange
        val sql = "select * from dtm.table1 OFFSET 2 FETCH NEXT 1 ROWS ONLY";
        SqlParser parser = getParser(sql);

        // act assert
        Assertions.assertThrows(SqlParseException.class, () -> parser.parseQuery());
    }

    @Test
    void shouldFailWhenOffsetAndLimitLast() {
        // arrange
        val sql = "select * from dtm.table1 OFFSET 2 LIMIT 1";
        SqlParser parser = getParser(sql);

        // act assert
        Assertions.assertThrows(SqlParseException.class, () -> parser.parseQuery());
    }

    @Test
    void shouldFailWhenOnlyOffset() {
        // arrange
        val sql = "select * from dtm.table1 OFFSET 2";
        SqlParser parser = getParser(sql);

        // act assert
        Assertions.assertThrows(SqlParseException.class, () -> parser.parseQuery());
    }

    @Test
    void shouldFailWhenOnlyDynamicOffset() {
        // arrange
        val sql = "select * from dtm.table1 OFFSET ?";
        SqlParser parser = getParser(sql);

        // act assert
        Assertions.assertThrows(SqlParseException.class, () -> parser.parseQuery());
    }

    @Test
    void shouldFailWhenDoubleLimit() {
        // arrange
        val sql = "select * from dtm.table1 LIMIT 1,2";
        SqlParser parser = getParser(sql);

        // act assert
        Assertions.assertThrows(SqlParseException.class, () -> parser.parseQuery());
    }

    @Test
    void shouldFailWhenDynamicDoubleLimit() {
        // arrange
        val sql = "select * from dtm.table1 LIMIT ?,?";
        SqlParser parser = getParser(sql);

        // act assert
        Assertions.assertThrows(SqlParseException.class, () -> parser.parseQuery());
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
