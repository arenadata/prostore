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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.dml;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.AdbCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.AdbCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.AdbSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.enrichment.*;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class AdbQueryEnrichmentServiceImplTest {

    QueryEnrichmentService adbQueryEnrichmentService;

    public AdbQueryEnrichmentServiceImplTest() {
        QueryExtendService queryExtender = new AdbCalciteDmlQueryExtendServiceImpl();

        CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
        calciteConfiguration.init();
        SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
            calciteConfiguration.ddlParserImplFactory()
        );

        AdbCalciteContextProvider contextProvider = new AdbCalciteContextProvider(
            parserConfig,
            new AdbCalciteSchemaFactory(new AdbSchemaFactory()));

        AdbQueryGeneratorImpl adbQueryGeneratorimpl = new AdbQueryGeneratorImpl(queryExtender, calciteConfiguration.adbSqlDialect());
        QueryParserService queryParserService = new AdbCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
        adbQueryEnrichmentService = new AdbQueryEnrichmentServiceImpl(
            queryParserService,
            adbQueryGeneratorimpl,
            contextProvider,
            new AdbSchemaExtenderImpl());

    }

    private static void assertGrep(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(data);
        assertTrue(matcher.find(), String.format("Expected: %s, Received: %s", regexp, data));
    }

    @Test
    void enrich() {
        List<String> result = new ArrayList<>();
        EnrichQueryRequest enrichQueryRequest =
            prepareRequestDeltaNum("select * from test_datamart.pso FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06'");
        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                log.debug(ar.toString());
                result.add("OK");
                async.complete();
            });

            async.awaitSuccess(7000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        log.info(result.get(0));
    }

    @Test
    void enrichWithDeltaNum() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
            "select *, (CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END) as c\n" +
                "  from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                "    from shares.accounts a\n" +
                "    left join shares.transactions t using(account_id)\n" +
                "   group by a.account_id, account_type\n" +
                ")x");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    assertGrep(result[0], "sys_from <= 1 AND sys_to >= 1");
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void enrichWithFinishedIn() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaFinishedIn(
            "SELECT account_id FROM shares.accounts");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    assertEquals(
                        "SELECT account_id FROM shares.accounts_history WHERE sys_to >= 0 AND (sys_to <= 0 AND sys_op = 1)",
                        result[0]
                    );
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void enrichWithStaticCaseExpressions() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
            "select a.account_id, (case when a.account_type = 'D' then 'ok' else 'not ok' end) as ss " +
                "from shares.accounts a ");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    assertGrep(result[0], "CASE WHEN account_type = 'D' THEN 'ok' ELSE 'not ok' END AS ss");
                    assertGrep(result[0], "sys_from <= 1 AND sys_to >= 1");
                }
                async.complete();
            });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void enrichWithDeltaInterval() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
            "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                "  from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                "    from shares.accounts a\n" +
                "    left join shares.transactions t using(account_id)\n" +
                "   group by a.account_id, account_type\n" +
                ")x");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    assertGrep(result[0], "sys_from >= 1 AND sys_from <= 5");
                    assertGrep(result[0], "sys_to <= 3 AND sys_op = 1");
                    assertGrep(result[0], "sys_to >= 2");
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void enrichWithNull() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
            "select account_id, null, null from shares.accounts");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    log.info(result[0]);
                    assertGrep(result[0], "NULL AS EXPR$1, NULL AS EXPR$2");
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void enrichWithLimit() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
            "select account_id from shares.accounts limit 50");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    log.info(result[0]);
                    assertGrep(result[0], "LIMIT 50");
                    async.complete();
                } else {
                    context.fail(ar.cause());
                }
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void enrichWithLimitAndOrder() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
            "select account_id from shares.accounts order by account_id limit 50");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    log.info(result[0]);
                    assertGrep(result[0], "ORDER BY account_id LIMIT 50");
                    async.complete();
                } else {
                    context.fail(ar.cause());
                }
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void enfichWithMultipleLogicalSchema() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchemas(
            "select * from accounts a " +
                "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                "JOIN test_datamart.transactions t ON t.account_id = a.account_id");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    assertGrep(result[0], "shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1");
                    assertGrep(result[0], "shares.accounts_actual WHERE sys_from <= 1");
                    assertGrep(result[0], "shares_2.accounts_history WHERE sys_from <= 1 AND sys_to >= 1");
                    assertGrep(result[0], "shares_2.accounts_actual WHERE sys_from <= 1");
                    assertGrep(result[0], "test_datamart.transactions_history WHERE sys_from <= 1 AND sys_to >= 1");
                    assertGrep(result[0], "test_datamart.transactions_actual WHERE sys_from <= 1");
                }
                async.complete();
            });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    private EnrichQueryRequest prepareRequestMultipleSchemas(String sql) {
        List<Datamart> schemas = Arrays.asList(
            getSchema("shares", true),
            getSchema("shares_2", false),
            getSchema("test_datamart", false));
        String requestSchema = schemas.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(requestSchema);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
            DeltaInformation.builder()
                .tableAlias("a")
                .deltaTimestamp("2019-12-23 15:15:14")
                .isLatestUncommittedDelta(false)
                .selectOnNum(1L)
                .selectOnInterval(null)
                .type(DeltaType.NUM)
                .schemaName(schemas.get(0).getMnemonic())
                .tableName(schemas.get(0).getEntities().get(0).getName())
                .pos(pos)
                .selectOnNum(1L)
                .build(),
            DeltaInformation.builder()
                .tableAlias("aa")
                .deltaTimestamp("2019-12-23 15:15:14")
                .isLatestUncommittedDelta(false)
                .selectOnNum(1L)
                .selectOnInterval(null)
                .type(DeltaType.NUM)
                .schemaName(schemas.get(1).getMnemonic())
                .tableName(schemas.get(1).getEntities().get(1).getName())
                .pos(pos)
                .build(),
            DeltaInformation.builder()
                .tableAlias("t")
                .deltaTimestamp("2019-12-23 15:15:14")
                .isLatestUncommittedDelta(false)
                .selectOnNum(1L)
                .selectOnInterval(null)
                .type(DeltaType.NUM)
                .schemaName(schemas.get(2).getMnemonic())
                .tableName(schemas.get(2).getEntities().get(1).getName())
                .pos(pos)
                .build()
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, schemas, Collections.emptyList());
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> schemas = Arrays.asList(getSchema("shares", true));
        String schemaName = schemas.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schemaName);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
            DeltaInformation.builder()
                .tableAlias("a")
                .deltaTimestamp("2019-12-23 15:15:14")
                .isLatestUncommittedDelta(false)
                .selectOnNum(1L)
                .selectOnInterval(null)
                .type(DeltaType.NUM)
                .schemaName(schemaName)
                .tableName(schemas.get(0).getEntities().get(0).getName())
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
                .tableName(schemas.get(0).getEntities().get(1).getName())
                .pos(pos)
                .build()
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, schemas, Collections.emptyList());
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaInterval(String sql) {
        List<Datamart> datamarts = Arrays.asList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schemaName);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
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
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts, Collections.emptyList());
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaFinishedIn(String sql) {
        List<Datamart> datamarts = Arrays.asList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schemaName);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
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
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts, Collections.emptyList());
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
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
