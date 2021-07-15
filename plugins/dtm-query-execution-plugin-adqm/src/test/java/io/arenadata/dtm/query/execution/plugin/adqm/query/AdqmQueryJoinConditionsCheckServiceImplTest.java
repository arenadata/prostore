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
package io.arenadata.dtm.query.execution.plugin.adqm.query;

import com.fasterxml.jackson.core.type.TypeReference;
import io.arenadata.dtm.calcite.adqm.configuration.AdqmCalciteConfiguration;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.factory.AdqmCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.factory.AdqmSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.service.AdqmCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.service.AdqmCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adqm.query.dto.AdqmCheckJoinRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckServiceImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.extractor.SqlJoinConditionExtractor;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.extractor.SqlJoinConditionExtractorImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.TestUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class AdqmQueryJoinConditionsCheckServiceImplTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final AdqmCalciteConfiguration calciteCoreConfiguration = new AdqmCalciteConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
            calciteCoreConfiguration.eddlParserImplFactory()
    );
    private final AdqmCalciteSchemaFactory calciteSchemaFactory = new AdqmCalciteSchemaFactory(new AdqmSchemaFactory());
    private final AdqmCalciteContextProvider calciteContextProvider = new AdqmCalciteContextProvider(parserConfig, calciteSchemaFactory);
    private final QueryParserService parserService = new AdqmCalciteDMLQueryParserService(calciteContextProvider, Vertx.vertx());
    private final SqlJoinConditionExtractor joinConditionExtractor = new SqlJoinConditionExtractorImpl();
    private AdqmQueryJoinConditionsCheckService conditionsCheckService;

    @BeforeEach
    void setUp() {
        conditionsCheckService = new AdqmQueryJoinConditionsCheckServiceImpl(joinConditionExtractor);
    }

    @Test
    void test0() {
        //success with subquery schema, 1 distr key, one alias
        test("SELECT * FROM (\n" +
                "SELECT * FROM dml.products p\n" +
                "    INNER JOIN dml.categories c on p.category_id = c.id\n" +
                "WHERE p.category_id > 5)", true);
    }

    @Test
    void test1() {
        //success with one schema, 1 distr key, one alias
        test("SELECT * FROM dml.products\n" +
                "    INNER JOIN dml.categories c on dml.products.id = c.id\n" +
                "    AND dml.products.product_name = c.category_name\n" +
                "WHERE dml.products.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test2() {
        //success with one schema, 1 distr key, two aliases
        test("SELECT * FROM dml.products p\n" +
                "    INNER JOIN dml.categories c on p.id = c.id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test3() {
        //success with one schema, 1 distr key, without aliases
        test("SELECT * FROM dml.products\n" +
                "    INNER JOIN dml.categories on dml.products.id = dml.categories.id\n" +
                "    AND dml.products.product_name = dml.categories.category_name\n" +
                "WHERE dml.products.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test4() {
        //success with one schema, 2 distr keys
        test("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.id\n" +
                "    AND p.distribution_id = c.distribution_id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test5() {
        //fail with one schema, with 1 distr key instead of 2
        test("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test6() {
        //fail with one schema, with 2 distr keys with OR
        test("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.id\n" +
                "    OR p.distribution_id = c.distribution_id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test7() {
        //fail with one schema, with 2 distr keys with incorrect order
        test("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.distribution_id\n" +
                "    AND p.distribution_id = c.id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test8() {
        //fail with one schema, with 2 distr keys with <>
        test("SELECT * FROM dml_2.products p\n" +
                "                  INNER JOIN dml_2.categories c on p.id = c.id\n" +
                "    AND p.distribution_id <> c.distribution_id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test9() {
        //fail with one schema, with 2 distr keys and condition with >
        test("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.id\n" +
                "    AND p.distribution_id > c.distribution_id\n" +
                "    AND p.product_name > c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test10() {
        //fail with one schema, with 2 distr keys and condition with between operator
        test("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.id\n" +
                "    AND p.distribution_id > c.distribution_id\n" +
                "    AND p.id between '1' AND '2'\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test11() {
        //fail with one schema, without distr keys
        test("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", false);
    }

    @Test
    void test12() {
        //fail with two schemas, with incorrect in second schema (1/2 distr key)
        testWithExceptions("SELECT * FROM dml.products p\n" +
                "    INNER JOIN dml.categories c on p.id = c.id AND p.product_name = c.category_name\n" +
                "    INNER JOIN dml_2.products p2 ON p2.category_id = c.id\n" +
                "    INNER JOIN dml_2.categories c2 on c2.id = p2.id AND p2.distribution_id = c2.distribution_id\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5");
    }

    @Test
    void test13() {
        //fail with incorrect join type
        testWithExceptions("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = CASE WHEN c.distribution_id = 1 THEN 1 ELSE 2 END\n" +
                "    AND p.distribution_id = c.id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5");
    }

    @Test
    void test14() {
        //fail with join sub query
        testWithExceptions("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN (select c.id, c.distribution_id, c.category_name from dml_2.categories c WHERE c.id in (1,2,3)) as s on p.id = s.distribution_id\n" +
                "    AND p.distribution_id = s.id\n" +
                "    AND p.product_name = s.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5");
    }

    @SneakyThrows
    void test(String sql, boolean expectedResult) {
        val testContext = new VertxTestContext();
        execute(sql, expectedResult, testContext);
        assertThat(testContext.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
        assertFalse(testContext.failed());
    }

    @SneakyThrows
    void testWithExceptions(String sql) {
        val testContext = new VertxTestContext();
        execute(sql, false, testContext);
        assertThat(testContext.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
    }

    private void execute(String sql, boolean expectedResult, VertxTestContext testContext) throws com.fasterxml.jackson.core.JsonProcessingException {
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val datamarts = DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml.json"), new TypeReference<List<Datamart>>() {
                });
        val parserRequest = new QueryParserRequest(sqlNode, datamarts);
        parserService.parse(parserRequest)
                .map(response -> conditionsCheckService.isJoinConditionsCorrect(new AdqmCheckJoinRequest(
                        response.getRelNode().rel, datamarts)))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        testContext.verify(() -> {
                            assertEquals(ar.result(), expectedResult);
                        }).completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
    }

    @SneakyThrows
    String loadTextFromFile(String path) {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }
}