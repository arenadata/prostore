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
package io.arenadata.dtm.query.execution.plugin.adg.base.service.client.impl.enrichment;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.factory.AdgCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.factory.AdgSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.AdgDmlQueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.AdgQueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.AdgQueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.AdgSchemaExtender;
import io.arenadata.dtm.query.execution.plugin.adg.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
class AdgQueryEnrichmentServiceImplTest {
    private static final String ENV_NAME = "local";
    private final QueryEnrichmentService enrichService;
    private final QueryParserService queryParserService;

    public AdgQueryEnrichmentServiceImplTest(Vertx vertx) {
        val calciteConfiguration = new AdgCalciteConfiguration();
        calciteConfiguration.init();
        val parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory());
        val contextProvider = new AdgCalciteContextProvider(
                parserConfig,
                new AdgCalciteSchemaFactory(new AdgSchemaFactory()));

        queryParserService = new AdgCalciteDMLQueryParserService(contextProvider, vertx);
        val helperTableNamesFactory = new AdgHelperTableNamesFactoryImpl();
        val queryExtendService = new AdgDmlQueryExtendService(helperTableNamesFactory);

        enrichService = new AdgQueryEnrichmentService(
                contextProvider,
                new AdgQueryGenerator(queryExtendService,
                        calciteConfiguration.adgSqlDialect()),
                new AdgSchemaExtender(helperTableNamesFactory));
    }

    private static void assertGrep(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(data);
        assertTrue(matcher.find(), String.format("Expected: %s, Received: %s", regexp, data));
    }

    @Test
    void enrichWithDeltaNum() {
        enrichWithGrep(prepareRequestDeltaNum("SELECT account_id FROM shares.accounts"),
                Arrays.asList("\"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1",
                        "\"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1"));
    }

    @Test
    void enrichWithFinishedIn() {
        enrichWithEquals(prepareRequestDeltaFinishedIn("SELECT account_id FROM shares.accounts"),
                Collections.singletonList("SELECT \"account_id\" FROM \"local__shares__accounts_history\" WHERE \"sys_to\" >= 0 AND (\"sys_to\" <= 0 AND \"sys_op\" = 1)"));
    }

    @Test
    void enrichWithDeltaInterval() {
        enrichWithGrep(prepareRequestDeltaInterval("select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                "OR (account_type = 'C' AND  amount <= 0) THEN 'OK    ' ELSE 'NOT OK' END\n" +
                "  from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                "    from shares.accounts a\n" +
                "    left join shares.transactions t using(account_id)\n" +
                "   group by a.account_id, account_type\n" +
                ")x"), Arrays.asList("\"local__shares__accounts_history\" where \"sys_from\" >= 1 and \"sys_from\" <= 5",
                "\"local__shares__accounts_actual\" where \"sys_from\" >= 1 and \"sys_from\" <= 5",
                "\"local__shares__transactions_history\" where \"sys_to\" >= 2",
                "\"sys_to\" <= 3 and \"sys_op\" = 1"));
    }

    @Test
    void enrichWithQuotes() {
        enrichWithGrep(prepareRequestDeltaNum("SELECT \"account_id\" FROM \"shares\".\"accounts\""),
                Arrays.asList("\"local__shares__accounts_history\" where \"sys_from\" <= 1 and \"sys_to\" >= 1",
                        "\"local__shares__accounts_actual\" where \"sys_from\" <= 1"));
    }

    @Test
    void enrichWithManyInKeyword(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT account_id FROM shares.accounts WHERE account_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)");

        // act assert
        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> enrichService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    // full of or conditions (not joins)
                    String expected = "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" WHERE \"account_id\" = 1 OR \"account_id\" = 2 OR (\"account_id\" = 3 OR (\"account_id\" = 4 OR \"account_id\" = 5)) OR (\"account_id\" = 6 OR (\"account_id\" = 7 OR \"account_id\" = 8) OR (\"account_id\" = 9 OR (\"account_id\" = 10 OR \"account_id\" = 11))) OR (\"account_id\" = 12 OR (\"account_id\" = 13 OR \"account_id\" = 14) OR (\"account_id\" = 15 OR (\"account_id\" = 16 OR \"account_id\" = 17)) OR (\"account_id\" = 18 OR (\"account_id\" = 19 OR \"account_id\" = 20) OR (\"account_id\" = 21 OR (\"account_id\" = 22 OR \"account_id\" = 23))))";
                    assertEquals(expected, ar.result());
                }).completeNow());
    }

    @Test
    void testEnrichWithSubquery(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT * FROM shares.accounts as b where b.account_id IN (select c.account_id from shares.transactions as c limit 1)");


        // act assert
        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> enrichService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(ar -> {
                    if (ar.failed()) {
                        testContext.failNow(ar.cause());
                        return;
                    }

                    testContext.verify(() -> {
                        assertEquals("SELECT * FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" WHERE \"account_id\" IN (SELECT \"account_id\" FROM (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_actual\" WHERE \"sys_from\" <= 1) AS \"t8\" LIMIT 1)", ar.result());
                    }).completeNow();
                });
    }

    @Test
    void enrichWithMultipleSchemas() {
        enrichWithGrep(prepareRequestMultipleSchema("SELECT a.account_id FROM accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id"),
                Arrays.asList(
                        "\"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1",
                        "\"local__shares__accounts_actual\" where \"sys_from\" <= 1",
                        "\"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2",
                        "\"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2",
                        "\"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2",
                        "\"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2"));
    }

    @Test
    void enrichWithCustomSelect(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT *, 0 FROM shares.accounts ORDER BY account_id");

        // act assert
        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> enrichService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    String expected = "SELECT \"account_id\", \"account_type\", 0 AS \"EXPR__2\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" ORDER BY \"account_id\"";
                    assertEquals(expected, ar.result());
                }).completeNow());
    }

    private void enrichWithGrep(EnrichQueryRequest enrichRequest,
                                List<String> expectedValues) {
        enrichWith(enrichRequest, expectedValues, true);
    }

    private void enrichWithEquals(EnrichQueryRequest enrichRequest,
                                  List<String> expectedValues) {
        enrichWith(enrichRequest, expectedValues, false);
    }

    private void enrichWith(EnrichQueryRequest enrichRequest,
                            List<String> expectedValues,
                            boolean grep) {
        String[] sqlResult = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            queryParserService.parse(new QueryParserRequest(enrichRequest.getQuery(), enrichRequest.getSchema()))
                    .compose(parserResponse -> enrichService.enrich(enrichRequest, parserResponse))
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            sqlResult[0] = ar.result();
                            if (grep) {
                                expectedValues.forEach(v -> assertGrep(sqlResult[0], v));
                            } else {
                                expectedValues.forEach(v -> assertEquals(v, sqlResult[0]));
                            }
                            async.complete();
                        } else {
                            sqlResult[0] = "-1";
                        }
                    });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    private EnrichQueryRequest prepareRequestMultipleSchema(String sql) {
        List<Datamart> datamarts = Arrays.asList(
                getSchema("shares", true),
                getSchema("shares_2", false),
                getSchema("test_datamart", false));
        String defaultSchema = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(defaultSchema)
                        .tableName(datamarts.get(0).getEntities().get(0).getName())
                        .pos(pos)
                        .selectOnNum(1L)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("aa")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(2L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(datamarts.get(1).getMnemonic())
                        .tableName(datamarts.get(1).getEntities().get(1).getName())
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(2L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(datamarts.get(2).getMnemonic())
                        .tableName(datamarts.get(2).getEntities().get(1).getName())
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

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(0).getName())
                        .pos(pos)
                        .selectOnNum(1L)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(1).getName())
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

    private EnrichQueryRequest prepareRequestDeltaInterval(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp(null)
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(new SelectOnInterval(1L, 5L))
                        .selectOnInterval(new SelectOnInterval(1L, 5L))
                        .type(DeltaType.STARTED_IN)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(0).getName())
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t")
                        .deltaTimestamp(null)
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(new SelectOnInterval(3L, 4L))
                        .selectOnInterval(new SelectOnInterval(3L, 4L))
                        .type(DeltaType.FINISHED_IN)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(1).getName())
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

    private EnrichQueryRequest prepareRequestDeltaFinishedIn(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Collections.singletonList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp(null)
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(new SelectOnInterval(1L, 1L))
                        .selectOnInterval(new SelectOnInterval(1L, 1L))
                        .type(DeltaType.FINISHED_IN)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(0).getName())
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
