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
package io.arenadata.dtm.query.execution.plugin.adg.dml.service;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.factory.AdgCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.factory.AdgSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.AdgDmlQueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.AdgQueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.AdgQueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.AdgSchemaExtender;
import io.arenadata.dtm.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.adg.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.DeleteRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdgDeleteServiceTest {

    private final AdgCalciteConfiguration calciteConfiguration = new AdgCalciteConfiguration();
    private final AdgHelperTableNamesFactoryImpl helperTableNamesFactory = new AdgHelperTableNamesFactoryImpl();
    private final QueryExtendService queryExtender = new AdgDmlQueryExtendService(helperTableNamesFactory);
    private final AdgCalciteContextProvider contextProvider = new AdgCalciteContextProvider(
            calciteConfiguration.configDdlParser(calciteConfiguration.ddlParserImplFactory()),
            new AdgCalciteSchemaFactory(new AdgSchemaFactory()));
    private final SqlDialect sqlDialect = calciteConfiguration.adgSqlDialect();
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
    private final AdgQueryGenerator queryGenerator = new AdgQueryGenerator(queryExtender, sqlDialect, relToSqlConverter);
    private final QueryEnrichmentService queryEnrichmentService = new AdgQueryEnrichmentService(contextProvider, queryGenerator, new AdgSchemaExtender(helperTableNamesFactory));

    @Mock
    private AdgQueryExecutorService executor;

    @Mock
    private AdgCartridgeClient cartridgeClient;

    @Captor
    private ArgumentCaptor<AdgTransferDataEtlRequest> transferRequestCaptor;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    private AdgDeleteService deleteService;

    @BeforeEach
    void setUp(Vertx vertx) {
        val queryParserService = new AdgCalciteDMLQueryParserService(contextProvider, vertx);
        deleteService = new AdgDeleteService(executor, cartridgeClient, helperTableNamesFactory, queryEnrichmentService, queryParserService);

        lenient().when(executor.executeUpdate(anyString(), any())).thenReturn(Future.succeededFuture());
        lenient().when(cartridgeClient.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenCondition(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"sys_op\") SELECT \"id\", 1 AS \"EXPR__1\" FROM (SELECT \"id\", \"col1\", \"col2\" FROM \"dev__datamart__abc_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\" FROM \"dev__datamart__abc_actual\" WHERE \"sys_from\" <= 0) AS \"t3\" WHERE \"id\" > 0 AND (\"col1\" = 0 AND \"col2\" <> 0)", sql);

                    verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
                    val transferDataEtlRequest = transferRequestCaptor.getValue();
                    assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
                    assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
                    assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
                    assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
                }).completeNow()));
    }

    @Test
    void shouldSuccessWhenNotNullableFields(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", false);

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"col2\",\"sys_op\") SELECT \"id\", \"col2\", 1 AS \"EXPR__2\" FROM (SELECT \"id\", \"col1\", \"col2\" FROM \"dev__datamart__abc_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\" FROM \"dev__datamart__abc_actual\" WHERE \"sys_from\" <= 0) AS \"t3\" WHERE \"id\" > 0 AND (\"col1\" = 0 AND \"col2\" <> 0)", sql);

                    verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
                    val transferDataEtlRequest = transferRequestCaptor.getValue();
                    assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
                    assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
                    assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
                    assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
                }).completeNow()));
    }

    @Test
    void shouldSuccessWhenNoCondition(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc", true);

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"sys_op\") SELECT \"id\", 1 AS \"EXPR__1\" FROM (SELECT \"id\", \"col1\", \"col2\" FROM \"dev__datamart__abc_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\" FROM \"dev__datamart__abc_actual\" WHERE \"sys_from\" <= 0) AS \"t3\"", sql);

                    verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
                    val transferDataEtlRequest = transferRequestCaptor.getValue();
                    assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
                    assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
                    assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
                    assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
                }).completeNow()));
    }

    @Test
    void shouldSuccessWhenConditionAndAlias(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc as a WHERE a.id > 0 AND a.col1 = 0 AND a.col2 <> 0", true);

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"sys_op\") SELECT \"id\", 1 AS \"EXPR__1\" FROM (SELECT \"id\", \"col1\", \"col2\" FROM \"dev__datamart__abc_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\" FROM \"dev__datamart__abc_actual\" WHERE \"sys_from\" <= 0) AS \"t3\" WHERE \"id\" > 0 AND (\"col1\" = 0 AND \"col2\" <> 0)", sql);

                    verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
                    val transferDataEtlRequest = transferRequestCaptor.getValue();
                    assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
                    assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
                    assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
                    assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
                }).completeNow()));
    }

    @Test
    void shouldFailWhenWrongQuery(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE unknown_col = 0", true);

        // act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecutorThrows(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeUpdate(any(), any())).thenThrow(new RuntimeException("Exception"));
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecutorFails(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeUpdate(any(), any())).thenReturn(Future.failedFuture("Failed"));
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void shouldFailWhenTransferThrows(VertxTestContext testContext) {
        // arrange
        reset(cartridgeClient);
        when(cartridgeClient.transferDataToScdTable(any())).thenThrow(new RuntimeException("Exception"));
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void shouldFailWhenTransferFails(VertxTestContext testContext) {
        // arrange
        reset(cartridgeClient);
        when(cartridgeClient.transferDataToScdTable(any())).thenReturn(Future.failedFuture("Failed"));
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    private DeleteRequest getDeleteRequest(String sql, boolean nullable) {
        val sqlNode = (SqlDelete) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val entity = Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.BIGINT)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.BIGINT)
                                .nullable(nullable)
                                .build()
                ))
                .build();

        val schema = Datamart.builder()
                .mnemonic("datamart")
                .entities(singletonList(entity))
                .isDefault(true)
                .build();

        return new DeleteRequest(UUID.randomUUID(), "dev", "datamart", entity, sqlNode, 1L, 0L, Collections.singletonList(schema), null);
    }
}