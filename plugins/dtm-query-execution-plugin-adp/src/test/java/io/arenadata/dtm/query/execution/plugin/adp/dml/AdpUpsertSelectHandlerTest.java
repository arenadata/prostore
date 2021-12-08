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
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeTemplates;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.factory.AdpCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.factory.AdpSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.service.AdpCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.service.AdpCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.dml.upsert.AdpUpsertSelectHandler;
import io.arenadata.dtm.query.execution.plugin.adp.enrichment.service.AdpDmlQueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adp.enrichment.service.AdpQueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adp.enrichment.service.AdpQueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adp.enrichment.service.AdpSchemaExtender;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.transfer.AdpTransferDataService;
import io.arenadata.dtm.query.execution.plugin.api.request.UpsertSelectRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.DEFINITION_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdpUpsertSelectHandlerTest {

    @Mock
    private DatabaseExecutor databaseExecutor;

    @Mock
    private AdpTransferDataService dataTransferService;

    private AdpUpsertSelectHandler adpUpsertSelectHandler;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @BeforeEach
    void setUp(Vertx vertx) {
        val calciteConfiguration = new CalciteConfiguration();
        val sqlDialect = calciteConfiguration.adpSqlDialect();
        val templateExtractor = new AdpQueryTemplateExtractor(DEFINITION_SERVICE, sqlDialect);
        val factory = calciteConfiguration.ddlParserImplFactory();
        val configParser = calciteConfiguration.configDdlParser(factory);
        val schemaFactory = new AdpSchemaFactory();
        val calciteSchemaFactory = new AdpCalciteSchemaFactory(schemaFactory);
        val contextProvider = new AdpCalciteContextProvider(configParser, calciteSchemaFactory);
        val parserService = new AdpCalciteDMLQueryParserService(contextProvider, vertx);
        val schemaExtender = new AdpSchemaExtender();
        val queryExtendService = new AdpDmlQueryExtendService();
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
        val queryGenerator = new AdpQueryGenerator(queryExtendService, sqlDialect, relToSqlConverter);
        val enrichmentService = new AdpQueryEnrichmentService(queryGenerator, contextProvider, schemaExtender);

        adpUpsertSelectHandler = new AdpUpsertSelectHandler(databaseExecutor, parserService, enrichmentService, templateExtractor, sqlDialect, dataTransferService);

        when(databaseExecutor.executeWithParams(anyString(), any(), anyList())).thenReturn(Future.succeededFuture());
        when(dataTransferService.transferData(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenWithoutCondition(VertxTestContext testContext) {
        val request = getUpsertRequest("UPSERT INTO datamart.abc (id, col1, col2, col3, col4) SELECT id, col1, col2, col3, col4 FROM datamart.src");

        adpUpsertSelectHandler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
                    assertEquals("INSERT INTO datamart.abc_staging (id, col1, col2, col3, col4, sys_op) (SELECT id, col1, col2, col3, col4, 0 AS sys_op FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", sqlCaptor.getValue());
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessAndNotCopyOffsetLimitParams(VertxTestContext testContext) {
        // arrange
        val extractedParams = Arrays.asList((SqlNode) SqlNodeTemplates.longLiteral(123L), SqlLiteral.createBoolean(true, SqlParserPos.ZERO), SqlNodeTemplates.longLiteral(1L), SqlNodeTemplates.longLiteral(2L));
        val extractedParamsTypes = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.BOOLEAN, SqlTypeName.BIGINT, SqlTypeName.BIGINT);
        val request = getUpsertRequest("UPSERT INTO datamart.abc (id) SELECT id FROM datamart.src WHERE id>? and col4=? ORDER BY id LIMIT ? OFFSET ?",
                null, extractedParams, extractedParamsTypes);

        // act
        adpUpsertSelectHandler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO datamart.abc_staging (id, sys_op) (SELECT id, 0 AS sys_op FROM (SELECT id, col1, col2, col3, col4 FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 WHERE id > 123 AND col4 = TRUE ORDER BY id LIMIT 1 OFFSET 2)", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenOnlyPkColumn(VertxTestContext testContext) {
        // arrange
        val request = getUpsertRequest("UPSERT INTO datamart.abc (id) SELECT id FROM datamart.src");

        // act
        adpUpsertSelectHandler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
                    assertEquals("INSERT INTO datamart.abc_staging (id, sys_op) (SELECT id, 0 AS sys_op FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", sqlCaptor.getValue());
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenWithoutColumns(VertxTestContext testContext) {
        // arrange
        val request = getUpsertRequest("UPSERT INTO datamart.abc SELECT id, col1, col2, col3, col4 FROM datamart.src");

        // act
        adpUpsertSelectHandler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
                    assertEquals("INSERT INTO datamart.abc_staging (id, col1, col2, col3, col4, sys_op) (SELECT id, col1, col2, col3, col4, 0 AS sys_op FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", sqlCaptor.getValue());
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenStarQuery(VertxTestContext testContext) {
        // arrange
        val request = getUpsertRequest("UPSERT INTO datamart.abc SELECT * FROM datamart.src");

        // act
        adpUpsertSelectHandler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
                    assertEquals("INSERT INTO datamart.abc_staging (id, col1, col2, col3, col4, sys_op) (SELECT id, col1, col2, col3, col4, 0 AS sys_op FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", sqlCaptor.getValue());
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecuteWithParamsFail(VertxTestContext testContext) {
        // arrange
        reset(databaseExecutor);
        reset(dataTransferService);
        when(databaseExecutor.executeWithParams(any(), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        val request = getUpsertRequest("UPSERT INTO datamart.abc (id) SELECT id FROM datamart.src");

        // act
        adpUpsertSelectHandler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Exception", ar.cause().getMessage());
                }).completeNow());
    }

    private UpsertSelectRequest getUpsertRequest(String sql) {
        return getUpsertRequest(sql, null, Collections.emptyList(), Collections.emptyList());
    }

    private UpsertSelectRequest getUpsertRequest(String sql, QueryParameters queryParameters, List<SqlNode> extractedParams, List<SqlTypeName> extractedParamsTypes) {
        SqlInsert sqlNode = (SqlInsert) DEFINITION_SERVICE.processingQuery(sql);
        SqlNode source = sqlNode.getSource();
        Entity entity = prepareEntity("abc");
        Entity entity2 = prepareEntity("src");
        Datamart datamart = Datamart.builder()
                .mnemonic("datamart")
                .isDefault(true)
                .entities(Arrays.asList(entity, entity2))
                .build();
        List<Datamart> datamarts = Arrays.asList(datamart);
        List<DeltaInformation> deltaInformations = Arrays.asList(DeltaInformation.builder()
                .selectOnNum(0L)
                .tableName("src")
                .schemaName("datamart")
                .type(DeltaType.WITHOUT_SNAPSHOT)
                .build());
        return new UpsertSelectRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, queryParameters, datamarts, deltaInformations, source, null, extractedParams, extractedParamsTypes);
    }

    private Entity prepareEntity(String name) {
        return Entity.builder()
                .schema("datamart")
                .name(name)
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col4")
                                .ordinalPosition(4)
                                .type(ColumnType.BOOLEAN)
                                .nullable(true)
                                .build()
                ))
                .build();
    }
}
