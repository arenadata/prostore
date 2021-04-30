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
package io.arenadata.dtm.query.execution.plugin.adqm.enrichment;

import com.fasterxml.jackson.core.type.TypeReference;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.impl.AdqmDmlQueryExtendServiceImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.impl.AdqmQueryEnrichmentServiceImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.impl.AdqmQueryGeneratorImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.impl.AdqmSchemaExtenderImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.service.AdqmCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.factory.AdqmCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.service.AdqmCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.factory.AdqmSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckServiceImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.TestUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
class AdqmQueryEnrichmentServiceImplTest {
    private static final int TIMEOUT_SECONDS = 120;
    private static final String ENV_NAME = "local";
    private static final List<Datamart> LOADED_DATAMARTS = loadDatamarts();
    private final QueryEnrichmentService enrichService;
    private final String[] expectedSqls;

    @SneakyThrows
    public AdqmQueryEnrichmentServiceImplTest() {
        val parserConfig = TestUtils.CALCITE_CONFIGURATION.configDdlParser(
                TestUtils.CALCITE_CONFIGURATION.ddlParserImplFactory());
        val contextProvider = new AdqmCalciteContextProvider(
                parserConfig,
                new AdqmCalciteSchemaFactory(new AdqmSchemaFactory()));

        val queryParserService = new AdqmCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
        val helperTableNamesFactory = new AdqmHelperTableNamesFactoryImpl();
        val queryExtendService = new AdqmDmlQueryExtendServiceImpl(helperTableNamesFactory);

        AdqmQueryJoinConditionsCheckService conditionsCheckService = mock(AdqmQueryJoinConditionsCheckServiceImpl.class);
        when(conditionsCheckService.isJoinConditionsCorrect(any())).thenReturn(true);
        enrichService = new AdqmQueryEnrichmentServiceImpl(
                queryParserService,
                contextProvider,
                new AdqmQueryGeneratorImpl(queryExtendService,
                        TestUtils.CALCITE_CONFIGURATION.adqmSqlDialect(), conditionsCheckService),
                new AdqmSchemaExtenderImpl(helperTableNamesFactory));

        expectedSqls = new String(Files.readAllBytes(Paths.get(getClass().getResource("/sql/expectedDmlSqls.sql").toURI())))
                .split("---");

    }

    @SneakyThrows
    private static List<Datamart> loadDatamarts() {
        return DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml.json"), new TypeReference<List<Datamart>>() {
                });
    }

    @SneakyThrows
    private static String loadTextFromFile(String path) {
        try (InputStream inputStream = AdqmQueryEnrichmentServiceImpl.class.getClassLoader().getResourceAsStream(path)) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    @Test
    void enrichWithDeltaNum() {
        enrich(prepareRequestDeltaNum("SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10"),
                expectedSqls[0], enrichService);
    }

    @Test
    void enrichWithDeltaNum2() {
        enrich(prepareRequestDeltaNum("SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id"),
                expectedSqls[1], enrichService);
    }

    @Test
    void enrichWithDeltaNum3() {
        enrich(prepareRequestDeltaNum("select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x"),
                expectedSqls[2], enrichService);
    }

    @Test
    void enrichWithDeltaNum4() {
        enrich(prepareRequestDeltaNum("SELECT * FROM shares.transactions as tran"),
                expectedSqls[3], enrichService);
    }

    @Test
    void enrichWithDeltaNum5() {
        enrich(prepareRequestDeltaNum("SELECT a1.account_id\n" +
                        "FROM (SELECT a2.account_id FROM shares.accounts a2 where a2.account_id = 12) a1\n" +
                        "    INNER JOIN shares.transactions t1 ON a1.account_id = t1.account_id"),
                expectedSqls[4], enrichService);
    }

    @Test
    void enrichWithDeltaNum6() {
        enrich(prepareRequestDeltaNum("SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id " +
                        "LIMIT 10"),
                expectedSqls[5], enrichService);
    }

    @Test
    void enrichCount() {
        enrich(prepareRequestDeltaNum("SELECT count(*) FROM shares.accounts"),
                expectedSqls[6], enrichService);
    }

    @Test
    void enrichWithDeltaNum9() {
        enrich(prepareRequestDeltaNum("SELECT * FROM shares.transactions where account_id = 1"),
                expectedSqls[7], enrichService);
    }

    @Test
    void enrichWithAggregate() {
        enrich(prepareRequestDeltaNumWithAggregate("SELECT min(int_col) as min_col, min(double_col) as max_col, varchar_col\n" +
                        "FROM dml.AGGREGATION_TABLE\n" +
                        "group by varchar_col\n" +
                        "order by varchar_col\n" +
                        "limit 2"),
                expectedSqls[8], enrichService);
    }

    @Test
    void enrichWithAggregate2() {
        enrich(prepareRequestDeltaNumWithAggregate("SELECT min(int_col) as min_col, min(double_col) as max_col, varchar_col, NULL as t1\n" +
                        "FROM dml.AGGREGATION_TABLE\n" +
                        "where varchar_col = 'ф'\n" +
                        "group by varchar_col\n" +
                        "limit 2"),
                expectedSqls[9], enrichService);
    }

    @Test
    void enrichWithSort() {
        enrich(prepareRequestDeltaNumWithSort("SELECT COUNT(c.category_name),\n" +
                "       c.category_name,\n" +
                "       sum(p.units_in_stock),\n" +
                "       c.id\n" +
                "FROM dml.products p\n" +
                "         JOIN dml.categories c on p.category_id = c.id\n" +
                "GROUP BY c.category_name, c.id\n" +
                "ORDER BY c.id\n" +
                "limit 5"),
            expectedSqls[10], enrichService);
    }
    @Test
    void enrichWithSort3() {
        enrich(prepareRequestDeltaNumWithSort("SELECT COUNT(dml.categories.category_name),\n" +
                "       dml.categories.category_name,\n" +
                "       dml.categories.id,\n" +
                "       sum(dml.products.units_in_stock)\n" +
                "FROM dml.products\n" +
                "         INNER JOIN dml.categories on dml.products.category_id = dml.categories.id\n" +
                "GROUP BY dml.categories.category_name, dml.categories.id\n" +
                "ORDER BY dml.categories.id limit 5"),
            expectedSqls[11], enrichService);
    }

    @Test
    void enrichWithSort4() {
        enrich(prepareRequestDeltaNumWithSort("SELECT COUNT(dml.categories.category_name),\n" +
                "       sum(dml.products.units_in_stock)\n" +
                "FROM dml.products\n" +
                "         INNER JOIN dml.categories on dml.products.category_id = dml.categories.id\n" +
                "GROUP BY dml.categories.category_name, dml.categories.id\n" +
                "ORDER BY dml.categories.id limit 5"),
            expectedSqls[12], enrichService);
    }

    @Test
    void enrichWithSort5() {
        enrich(prepareRequestDeltaNumWithSort("SELECT c.id \n" +
                "FROM dml.products p\n" +
                "         JOIN dml.categories c on p.category_id = c.id\n" +
                "ORDER BY c.id"),
            expectedSqls[13], enrichService);
    }

    @Test
    @Disabled("Needed refactoring AdqmCalciteDmlQueryExtendServiceImpl.processProject")
    void enrichWithSort6() {
        enrich(prepareRequestDeltaNumWithSort("SELECT *\n" +
                "from dml.categories c\n" +
                "         JOIN (select * from  dml.products) p on c.id = p.category_id\n" +
                "ORDER by c.id, p.product_name desc"),
            expectedSqls[13], enrichService);
    }

    @SneakyThrows
    private void enrich(EnrichQueryRequest enrichRequest,
                        String expectedSql,
                        QueryEnrichmentService service) {
        val testContext = new VertxTestContext();
        val actual = new String[]{""};

        service.enrich(enrichRequest)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        actual[0] = ar.result();
                        testContext.completeNow();
                    } else {
                        actual[0] = ar.cause().getMessage();
                        testContext.failNow(ar.cause());
                        log.error("ERROR", ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertEquals(expectedSql.trim(), actual[0].trim(),
                String.format("Expected: %s\n Actual: %s", expectedSql.trim(), actual[0].trim()));
    }

    private EnrichQueryRequest prepareRequestDeltaNumWithSort(String sql) {
        String schemaName = LOADED_DATAMARTS.get(0).getMnemonic();
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
            DeltaInformation.builder()
                .tableAlias("p")
                .deltaTimestamp("2019-12-23 15:15:14")
                .isLatestUncommittedDelta(false)
                .selectOnNum(1L)
                .selectOnInterval(null)
                .type(DeltaType.NUM)
                .schemaName(schemaName)
                .tableName(LOADED_DATAMARTS.get(0).getEntities().get(1).getName())
                .build(),
            DeltaInformation.builder()
                .tableAlias("c")
                .deltaTimestamp("2019-12-23 15:15:14")
                .isLatestUncommittedDelta(false)
                .selectOnNum(1L)
                .selectOnInterval(null)
                .type(DeltaType.NUM)
                .schemaName(schemaName)
                .tableName(LOADED_DATAMARTS.get(0).getEntities().get(2).getName())
                .build(),
            DeltaInformation.builder()
                .tableAlias("c")
                .deltaTimestamp("2019-12-23 15:15:14")
                .isLatestUncommittedDelta(false)
                .selectOnNum(1L)
                .selectOnInterval(null)
                .type(DeltaType.NUM)
                .schemaName(schemaName)
                .tableName(LOADED_DATAMARTS.get(0).getEntities().get(2).getName())
                .build(),
            DeltaInformation.builder()
                .tableAlias("c")
                .deltaTimestamp("2019-12-23 15:15:14")
                .isLatestUncommittedDelta(false)
                .selectOnNum(1L)
                .selectOnInterval(null)
                .type(DeltaType.NUM)
                .schemaName(schemaName)
                .tableName(LOADED_DATAMARTS.get(0).getEntities().get(2).getName())
                .build(),
            DeltaInformation.builder()
                .tableAlias("c")
                .deltaTimestamp("2019-12-23 15:15:14")
                .isLatestUncommittedDelta(false)
                .selectOnNum(1L)
                .selectOnInterval(null)
                .type(DeltaType.NUM)
                .schemaName(schemaName)
                .tableName(LOADED_DATAMARTS.get(0).getEntities().get(2).getName())
                .build()
        );
        return EnrichQueryRequest.builder()
            .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
            .deltaInformations(deltaInforamtions)
            .envName(ENV_NAME)
            .schema(loadDatamarts())
            .build();
    }

    private EnrichQueryRequest prepareRequestDeltaNumWithAggregate(String sql) {
        String schemaName = LOADED_DATAMARTS.get(0).getMnemonic();
        String tableName = LOADED_DATAMARTS.get(0).getEntities().get(0).getName();
        List<DeltaInformation> deltaInforamtions = Collections.singletonList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .build()
        );
        return EnrichQueryRequest.builder()
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .schema(loadDatamarts())
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        String tableName = datamarts.get(0).getEntities().get(0).getName();
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t1")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t2")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t3")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .pos(pos)
                        .build()
        );
        return EnrichQueryRequest.builder()
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .schema(datamarts)
                .build();
    }

    private Datamart getSchema(String schemaName, boolean isDefault) {
        Entity accounts = Entity.builder()
                .schema(schemaName)
                .name("accounts")
                .build();
        List<EntityField> accAttrs = Arrays.asList(
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("account_id")
                        .ordinalPosition(1)
                        .shardingOrder(1)
                        .primaryOrder(1)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.VARCHAR)
                        .name("account_type")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(1)
                        .build()
        );
        accounts.setFields(accAttrs);

        Entity transactions = Entity.builder()
                .schema(schemaName)
                .name("transactions")
                .build();

        List<EntityField> trAttr = Arrays.asList(
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("transaction_id")
                        .ordinalPosition(1)
                        .shardingOrder(1)
                        .primaryOrder(1)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.DATE)
                        .name("transaction_date")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(true)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("account_id")
                        .ordinalPosition(3)
                        .shardingOrder(1)
                        .primaryOrder(2)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("amount")
                        .ordinalPosition(4)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build()
        );

        transactions.setFields(trAttr);

        return new Datamart(schemaName, isDefault, Arrays.asList(transactions, accounts));
    }
}
