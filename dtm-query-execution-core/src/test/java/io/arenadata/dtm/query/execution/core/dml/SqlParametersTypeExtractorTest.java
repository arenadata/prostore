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
package io.arenadata.dtm.query.execution.core.dml;

import com.fasterxml.jackson.core.type.TypeReference;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dml.service.SqlParametersTypeExtractor;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.dml.service.impl.SqlParametersTypeExtractorImpl;
import io.arenadata.dtm.query.execution.core.utils.TestUtils;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class SqlParametersTypeExtractorTest {

    private static final String SQL_WHERE_BETWEEN = "SELECT * FROM all_types_table\n" +
            "WHERE date_col = ?\n" +
            "AND timestamp_col > ?\n" +
            "AND int_col BETWEEN ? AND ?";
    private static final String SQL_WHERE_IN = "SELECT * FROM all_types_table " +
            "WHERE time_col IN (?, ?, ?)";
    private static final String SQL_CASE = "SELECT date_col,\n" +
            "CASE\n" +
            "when id = ? THEN 'case 1'\n" +
            "ELSE 'case other'\n" +
            "END AS id_case\n" +
            "FROM all_types_table";
    private static final String SQL_JOIN = "SELECT a1.date_col, a2.int_col FROM accounts a1\n" +
            "JOIN all_types_table a2 ON a1.id = a2.id AND a1.date_col = ?\n" +
            "WHERE a2.timestamp_col > ?";

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlParser.Config configParser = calciteConfiguration.configEddlParser(calciteConfiguration.getSqlParserFactory());
    private final CoreCalciteSchemaFactory calciteSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CoreCalciteContextProvider calciteContextProvider = new CoreCalciteContextProvider(configParser, calciteSchemaFactory);
    private final QueryParserService parserService = new CoreCalciteDMLQueryParserService(calciteContextProvider, Vertx.vertx());
    private final SqlParametersTypeExtractor parametersTypeExtractor = new SqlParametersTypeExtractorImpl();

    @Test
    void testExtractWhereBetween() {
        List<SqlTypeName> expectedResult = Arrays.asList(SqlTypeName.DATE, SqlTypeName.TIMESTAMP, SqlTypeName.INTEGER, SqlTypeName.INTEGER);
        test(SQL_WHERE_BETWEEN, expectedResult);
    }

    @Test
    void testExtractWhereIn() {
        List<SqlTypeName> expectedResult = Arrays.asList(SqlTypeName.TIME);
        test(SQL_WHERE_IN, expectedResult);
    }

    @Test
    void testExtractCase() {
        List<SqlTypeName> expectedResult = Arrays.asList(SqlTypeName.INTEGER);
        test(SQL_CASE, expectedResult);
    }

    @Test
    void testExtractJoin() {
        List<SqlTypeName> expectedResult = Arrays.asList(SqlTypeName.DATE, SqlTypeName.TIMESTAMP);
        test(SQL_JOIN, expectedResult);
    }

    @SneakyThrows
    private void test(String sql, List<SqlTypeName> expectedResult) {
        val testContext = new VertxTestContext();
        val datamarts = DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/type_extractor_schema.json"), new TypeReference<List<Datamart>>() {
                });
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val parserRequest = new QueryParserRequest(sqlNode, datamarts);
        parserService.parse(parserRequest)
                .map(response -> parametersTypeExtractor.extract(response.getRelNode().rel))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        log.info("Result columns: {}", ar.result());
                        assertEquals(expectedResult, ar.result());
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @SneakyThrows
    String loadTextFromFile(String path) {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }
}
