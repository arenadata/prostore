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
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.impl.AdgDmlQueryExtendServiceImpl;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.impl.AdgQueryEnrichmentServiceImpl;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.impl.AdgQueryGeneratorImpl;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.impl.AdgSchemaExtenderImpl;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.factory.AdgCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.factory.AdgSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.utils.TestUtils;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import lombok.val;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdgQueryEnrichmentServiceImplTest {
    private static final String ENV_NAME = "local";
    private final QueryEnrichmentService enrichService;

    public AdgQueryEnrichmentServiceImplTest() {
        val calciteConfiguration = new AdgCalciteConfiguration();
        calciteConfiguration.init();
        val parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory());
        val contextProvider = new AdgCalciteContextProvider(
                parserConfig,
                new AdgCalciteSchemaFactory(new AdgSchemaFactory()));

        val queryParserService = new AdgCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
        val helperTableNamesFactory = new AdgHelperTableNamesFactoryImpl();
        val queryExtendService = new AdgDmlQueryExtendServiceImpl(helperTableNamesFactory);

        enrichService = new AdgQueryEnrichmentServiceImpl(
                queryParserService,
                contextProvider,
                new AdgQueryGeneratorImpl(queryExtendService,
                        calciteConfiguration.adgSqlDialect()),
                new AdgSchemaExtenderImpl(helperTableNamesFactory));
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
            enrichService.enrich(enrichRequest)
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
