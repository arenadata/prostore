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
package io.arenadata.dtm.query.execution.plugin.adp.dml;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.factory.AdpCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.factory.AdpSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.service.AdpCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.service.AdpCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.service.AdpCalciteDefinitionService;
import io.arenadata.dtm.query.execution.plugin.adp.enrichment.service.AdpDmlQueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adp.enrichment.service.AdpQueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adp.enrichment.service.AdpQueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adp.enrichment.service.AdpSchemaExtender;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryGenerator;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@ExtendWith(VertxExtension.class)
class AdpQueryEnrichmentServiceTest {

    public static final String SHARES_SCHEMA_NAME = "shares";
    public static final String SHARES_2_SCHEMA_NAME = "shares_2";
    public static final String TEST_DATAMART_SCHEMA_NAME = "test_datamart";
    public static final String ENV_NAME = "test";
    private final SqlDialect dialect = new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.POSTGRESQL)
            .withIdentifierQuoteString("")
            .withUnquotedCasing(Casing.TO_LOWER)
            .withCaseSensitive(false)
            .withQuotedCasing(Casing.UNCHANGED));
    private final SqlParser.Config configParser = SqlParser.configBuilder()
            .setParserFactory(new CalciteCoreConfiguration().eddlParserImplFactory())
            .setConformance(SqlConformanceEnum.DEFAULT)
            .setCaseSensitive(false)
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.TO_LOWER)
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .build();
    private final DefinitionService<SqlNode> definitionService = new AdpCalciteDefinitionService(configParser);
    private final QueryExtendService queryExtender = new AdpDmlQueryExtendService();
    private final AdpCalciteContextProvider contextProvider = new AdpCalciteContextProvider(
            configParser,
            new AdpCalciteSchemaFactory(new AdpSchemaFactory()));
    private final QueryGenerator adpQueryGenerator = new AdpQueryGenerator(queryExtender, dialect);
    private final QueryParserService queryParserService = new AdpCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
    private final QueryEnrichmentService adpQueryEnrichmentService = new AdpQueryEnrichmentService(
            adpQueryGenerator,
            contextProvider,
            new AdpSchemaExtender());

    @Test
    void testEnrichWithCountAndLimit(VertxTestContext testContext) {
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT COUNT(*) AS C FROM shares.accounts LIMIT 100");

        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals("SELECT * FROM (SELECT COUNT(*) AS c FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t2 LIMIT 100",
                            result);
                    testContext.completeNow();
                })));
    }

    @Test
    void enrichWithDeltaNum(VertxTestContext testContext) {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "select *, (CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END) as c\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x");

        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertTrue(result.contains("sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1"));
                    testContext.completeNow();
                })));
    }

    @Test
    void enrich(VertxTestContext testContext) {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaFinishedIn(
                "SELECT account_id FROM shares.accounts");

        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals("SELECT account_id FROM shares.accounts_actual WHERE COALESCE(sys_to, 9223372036854775807) >= 0 AND (COALESCE(sys_to, 9223372036854775807) <= 0 AND sys_op = 1)",
                            result);
                    testContext.completeNow();
                })));
    }

    @Test
    void enrichWithStaticCaseExpressions(VertxTestContext testContext) {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "select a.account_id, (case when a.account_type = 'D' then 'ok' else 'not ok' end) as ss " +
                        "from shares.accounts a ");

        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertTrue(result.contains("CASE WHEN account_type = 'D' THEN 'ok' ELSE 'not ok' END AS ss"));
                    assertTrue(result.contains("sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1"));
                    testContext.completeNow();
                })));
    }

    @Test
    void enrichWithDeltaInterval(VertxTestContext testContext) {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x");

        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertTrue(result.contains("sys_from >= 1 AND sys_from <= 5"));
                    assertTrue(result.contains("COALESCE(sys_to, 9223372036854775807) <= 3 AND sys_op = 1"));
                    assertTrue(result.contains("COALESCE(sys_to, 9223372036854775807) >= 2"));
                    testContext.completeNow();
                })));
    }

    @Test
    void enrichWithNull(VertxTestContext testContext) {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select account_id, null, null from shares.accounts");

        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertTrue(result.contains("NULL AS EXPR__1, NULL AS EXPR__2"));
                    testContext.completeNow();
                })));
    }

    @Test
    void enrichWithLimit(VertxTestContext testContext) {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select account_id from shares.accounts limit 50");

        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertTrue(result.contains("LIMIT 50"));
                    testContext.completeNow();
                })));
    }

    @Test
    void enrichWithLimitAndOrder(VertxTestContext testContext) {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select account_id from shares.accounts order by account_id limit 50");

        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertTrue(result.contains("ORDER BY account_id LIMIT 50"));
                    testContext.completeNow();
                })));
    }
    
    @Test
    void testEnrichWithSubquery(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT * FROM shares.accounts as b where b.account_id IN (select c.account_id from shares.transactions as c limit 1)");


        // act assert
        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(ar -> {
                    if (ar.failed()) {
                        testContext.failNow(ar.cause());
                        return;
                    }

                    testContext.verify(() -> {
                        assertEquals("SELECT * FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 WHERE account_id IN (SELECT account_id FROM shares.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1 LIMIT 1)", ar.result());
                    }).completeNow();
                });
    }

    @Test
    void enrichWithMultipleLogicalSchema(VertxTestContext testContext) {
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchemas(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id");

        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertTrue(result.contains("shares.accounts_actual WHERE sys_from <= 1"));
                    assertTrue(result.contains("shares_2.accounts_actual WHERE sys_from <= 1"));
                    assertTrue(result.contains("test_datamart.transactions_actual WHERE sys_from <= 1"));
                    testContext.completeNow();
                })));
    }

    @Test
    void enrichWithCustomSelect(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT *, 0 FROM shares.accounts ORDER BY account_id");

        // act assert
        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adpQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    String expected = "SELECT account_id, account_type, 0 AS EXPR__2 FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1 ORDER BY account_id";
                    assertEquals(expected, ar.result());
                }).completeNow());
    }

    private EnrichQueryRequest prepareRequestMultipleSchemas(String sql) {
        List<Datamart> schemas = Arrays.asList(
                getSchema(SHARES_SCHEMA_NAME, true),
                getSchema(SHARES_2_SCHEMA_NAME, false),
                getSchema(TEST_DATAMART_SCHEMA_NAME, false));
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
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
        );
        return EnrichQueryRequest.builder()
                .deltaInformations(deltaInforamtions)
                .envName("test")
                .query(definitionService.processingQuery(sql))
                .schema(schemas)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> schemas = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
        String schemaName = schemas.get(0).getMnemonic();
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
        );

        return EnrichQueryRequest.builder()
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .query(definitionService.processingQuery(sql))
                .schema(schemas)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaInterval(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
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
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .query(definitionService.processingQuery(sql))
                .schema(datamarts)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaFinishedIn(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
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
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .query(definitionService.processingQuery(sql))
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
