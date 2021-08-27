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
package io.arenadata.dtm.query.execution.plugin.adp.db.service;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.exception.LlrDatasourceException;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgException;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.createAllTypesTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Testcontainers
@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdpQueryExecutorTest {

    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";
    private static final String DB_NAME = "datamart";
    private static final String INVALID_DB = "non_existent_db";
    private static final String TABLE = "test_table";
    private static final String SELECT_ALL = "SELECT * FROM %s.%s";

    private static GenericContainer<?> container;

    @Mock
    private SqlTypeConverter fromSqlConverter;

    @Mock
    private SqlTypeConverter toSqlConverter;

    private AdpQueryExecutor queryExecutor;


    @BeforeAll
    static void init() {
        container = new PostgreSQLContainer<>("postgres:11.1")
                .withDatabaseName(DB_NAME)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withInitScript("init_script.sql")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(
                        AdpQueryExecutorTest.class)));
        container.start();
    }

    @BeforeEach
    void setup(Vertx vertx) {
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(container.getMappedPort(5432))
                .setHost(container.getHost())
                .setDatabase(DB_NAME)
                .setUser(USERNAME)
                .setPassword(PASSWORD);
        PgPool pgPool = PgPool.pool(vertx, connectOptions, new PoolOptions());
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);
    }

    @Test
    void execute_Success(VertxTestContext testContext) {
        val expectedResult = new ArrayList<>();
        val row = new HashMap<>();
        row.put("id", 12L);
        val typesValues = expectedTypeValues();
        typesValues.forEach((key, value) ->
                row.put(key.name().toLowerCase() + "_col", value));
        expectedResult.add(row);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.execute(sql)
                .onComplete(testContext.succeeding(result ->
                        testContext.verify(() ->
                                assertEquals(expectedResult, result))
                                .completeNow()));
    }

    @Test
    void execute_InvalidQuery_Fail(VertxTestContext testContext) {
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);

        queryExecutor.execute(sql)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof PgException))
                                .completeNow()));
    }

    @Test
    void execute_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.execute(sql)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof UnknownHostException))
                                .completeNow()));
    }

    @Test
    void execute_EmptyMetadata_Success(VertxTestContext testContext) {
        val expectedResult = new ArrayList<>();
        val row = new HashMap<>();
        row.put("id", 12L);
        val typesValues = expectedTypeValues();
        typesValues.forEach((key, value) ->
                row.put(key.name().toLowerCase() + "_col", value));
        expectedResult.add(row);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.execute(sql, Collections.emptyList())
                .onComplete(testContext.succeeding(result ->
                        testContext.verify(() ->
                                assertEquals(expectedResult, result))
                                .completeNow()));
    }

    @Test
    void execute_EmptyMetadata_InvalidQuery_Fail(VertxTestContext testContext) {
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);

        queryExecutor.execute(sql, Collections.emptyList())
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof PgException))
                                .completeNow()));
    }

    @Test
    void execute_EmptyMetadata_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.execute(sql, Collections.emptyList())
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof UnknownHostException))
                                .completeNow()));
    }

    @Test
    void execute_OneColumnMetadata_Success(VertxTestContext testContext) {
        val columnName = "varchar_col";
        val expectedResult = new ArrayList<>();
        val row = new HashMap<>();
        row.put(columnName, "varchar");
        expectedResult.add(row);
        val columnMetadata = new ColumnMetadata(columnName, ColumnType.VARCHAR);

        when(fromSqlConverter.convert(any(), any())).thenReturn("varchar");
        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.execute(sql, Collections.singletonList(columnMetadata))
                .onComplete(testContext.succeeding(result ->
                        testContext.verify(() ->
                                assertEquals(expectedResult, result))
                                .completeNow()));
    }

    @Test
    void execute_OneColumnMetadata_InvalidQuery_Fail(VertxTestContext testContext) {
        val columnMetadata = new ColumnMetadata("varchar_col", ColumnType.VARCHAR);
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);

        queryExecutor.execute(sql, Collections.singletonList(columnMetadata))
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof PgException))
                                .completeNow()));
    }

    @Test
    void execute_OneColumnMetadata_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val columnMetadata = new ColumnMetadata("varchar_col", ColumnType.VARCHAR);
        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.execute(sql, Collections.singletonList(columnMetadata))
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof UnknownHostException))
                                .completeNow()));
    }

    @Test
    void execute_AllColumnsMetadata_Success(VertxTestContext testContext) {
        val entity = createAllTypesTable();
        val metadata = entity.getFields().stream()
                .map(field -> new ColumnMetadata(field.getName(), field.getType()))
                .collect(Collectors.toList());

        val expectedResult = new ArrayList<>();
        val row = new HashMap<>();
        row.put("id", 12L);
        val typesValues = expectedTypeValues();
        typesValues.forEach((key, value) -> {
            when(fromSqlConverter.convert(eq(key), any())).thenReturn(value);
            row.put(key.name().toLowerCase() + "_col", value);
        });
        expectedResult.add(row);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.execute(sql, metadata)
                .onComplete(testContext.succeeding(result ->
                        testContext.verify(() ->
                                assertEquals(expectedResult, result))
                                .completeNow()));
    }

    @Test
    void execute_AllColumnsMetadata_InvalidQuery_Fail(VertxTestContext testContext) {
        val entity = createAllTypesTable();
        val metadata = entity.getFields().stream()
                .map(field -> new ColumnMetadata(field.getName(), field.getType()))
                .collect(Collectors.toList());
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);

        queryExecutor.execute(sql, metadata)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof PgException))
                                .completeNow()));
    }

    @Test
    void execute_AllColumnsMetadata_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val entity = createAllTypesTable();
        val metadata = entity.getFields().stream()
                .map(field -> new ColumnMetadata(field.getName(), field.getType()))
                .collect(Collectors.toList());
        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.execute(sql, metadata)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof UnknownHostException))
                                .completeNow()));
    }

    @Test
    void executeWithCursor_EmptyMetadata_Success(VertxTestContext testContext) {
        val expectedResult = new ArrayList<>();
        val row = new HashMap<>();
        row.put("id", 12L);
        val typesValues = expectedTypeValues();
        typesValues.forEach((key, value) -> row.put(key.name().toLowerCase() + "_col", value));
        expectedResult.add(row);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.executeWithCursor(sql, Collections.emptyList())
                .onComplete(testContext.succeeding(result ->
                        testContext.verify(() ->
                                assertEquals(expectedResult, result))
                                .completeNow()));
    }

    @Test
    void executeWithCursor_EmptyMetadata_InvalidQuery_Fail(VertxTestContext testContext) {
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);

        queryExecutor.executeWithCursor(sql, Collections.emptyList())
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof PgException))
                                .completeNow()));
    }

    @Test
    void executeWithCursor_EmptyMetadata_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.executeWithCursor(sql, Collections.emptyList())
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof UnknownHostException))
                                .completeNow()));
    }

    @Test
    void executeWithCursor_OneColumnMetadata_Success(VertxTestContext testContext) {
        val columnName = "varchar_col";
        val expectedResult = new ArrayList<>();
        val row = new HashMap<>();
        row.put(columnName, "varchar");
        expectedResult.add(row);
        val columnMetadata = new ColumnMetadata(columnName, ColumnType.VARCHAR);

        when(fromSqlConverter.convert(any(), any())).thenReturn("varchar");
        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.executeWithCursor(sql, Collections.singletonList(columnMetadata))
                .onComplete(testContext.succeeding(result ->
                        testContext.verify(() ->
                                assertEquals(expectedResult, result))
                                .completeNow()));
    }

    @Test
    void executeWithCursor_OneColumnMetadata_InvalidQuery_Fail(VertxTestContext testContext) {
        val columnMetadata = new ColumnMetadata("varchar_col", ColumnType.VARCHAR);
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);

        queryExecutor.executeWithCursor(sql, Collections.singletonList(columnMetadata))
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof PgException))
                                .completeNow()));
    }

    @Test
    void executeWithCursor_OneColumnMetadata_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val columnMetadata = new ColumnMetadata("varchar_col", ColumnType.VARCHAR);
        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.executeWithCursor(sql, Collections.singletonList(columnMetadata))
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof UnknownHostException))
                                .completeNow()));
    }

    @Test
    void executeWithCursor_AllColumnsMetadata_Success(VertxTestContext testContext) {
        val entity = createAllTypesTable();
        val metadata = entity.getFields().stream()
                .map(field -> new ColumnMetadata(field.getName(), field.getType()))
                .collect(Collectors.toList());

        val expectedResult = new ArrayList<>();
        val row = new HashMap<>();
        row.put("id", 12L);
        val typesValues = expectedTypeValues();
        typesValues.forEach((key, value) -> {
            when(fromSqlConverter.convert(eq(key), any())).thenReturn(value);
            row.put(key.name().toLowerCase() + "_col", value);
        });
        expectedResult.add(row);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.executeWithCursor(sql, metadata)
                .onComplete(testContext.succeeding(result ->
                        testContext.verify(() ->
                                assertEquals(expectedResult, result))
                                .completeNow()));
    }

    @Test
    void executeWithCursor_AllColumnsMetadata_InvalidQuery_Fail(VertxTestContext testContext) {
        val entity = createAllTypesTable();
        val metadata = entity.getFields().stream()
                .map(field -> new ColumnMetadata(field.getName(), field.getType()))
                .collect(Collectors.toList());
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);

        queryExecutor.executeWithCursor(sql, metadata)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof PgException))
                                .completeNow()));
    }

    @Test
    void executeWithCursor_AllColumnsMetadata_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val entity = createAllTypesTable();
        val metadata = entity.getFields().stream()
                .map(field -> new ColumnMetadata(field.getName(), field.getType()))
                .collect(Collectors.toList());
        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.executeWithCursor(sql, metadata)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof UnknownHostException))
                                .completeNow()));
    }

    @Test
    void executeInTransaction_OneQuery_Success(VertxTestContext testContext) {
        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);
        List<PreparedStatementRequest> requests = Collections.singletonList(PreparedStatementRequest.onlySql(sql));

        queryExecutor.executeInTransaction(requests)
                .onComplete(ar -> testContext.verify(() ->
                        assertTrue(ar.succeeded()))
                        .completeNow());
    }

    @Test
    void executeInTransaction_OneQuery_InvalidQuery_Fail(VertxTestContext testContext) {
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);
        List<PreparedStatementRequest> requests = Collections.singletonList(PreparedStatementRequest.onlySql(sql));

        queryExecutor.executeInTransaction(requests)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof LlrDatasourceException))
                                .completeNow()));
    }

    @Test
    void executeInTransaction_OneQuery_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);
        List<PreparedStatementRequest> requests = Collections.singletonList(PreparedStatementRequest.onlySql(sql));

        queryExecutor.executeInTransaction(requests)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof LlrDatasourceException))
                                .completeNow()));
    }

    @Test
    void executeInTransaction_EmptyRequest_Success(VertxTestContext testContext) {
        queryExecutor.executeInTransaction(Collections.emptyList())
                .onComplete(ar ->
                        testContext.verify(() ->
                                assertTrue(ar.succeeded()))
                                .completeNow());
    }

    @Test
    void executeInTransaction_EmptyRequest_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        queryExecutor.executeInTransaction(Collections.emptyList())
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof LlrDatasourceException))
                                .completeNow()));
    }

    @Test
    void executeInTransaction_MultipleQuery_Success(VertxTestContext testContext) {
        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);
        List<PreparedStatementRequest> requests = Arrays.asList(PreparedStatementRequest.onlySql(sql),
                PreparedStatementRequest.onlySql(sql));

        queryExecutor.executeInTransaction(requests)
                .onComplete(ar ->
                        testContext.verify(() ->
                                assertTrue(ar.succeeded()))
                                .completeNow());
    }

    @Test
    void executeInTransaction_MultipleQuery_InvalidQuery_Fail(VertxTestContext testContext) {
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);
        List<PreparedStatementRequest> requests = Arrays.asList(PreparedStatementRequest.onlySql(sql),
                PreparedStatementRequest.onlySql(sql));

        queryExecutor.executeInTransaction(requests)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof LlrDatasourceException))
                                .completeNow()));
    }

    @Test
    void executeInTransaction_MultipleQuery_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);
        List<PreparedStatementRequest> requests = Arrays.asList(PreparedStatementRequest.onlySql(sql),
                PreparedStatementRequest.onlySql(sql));

        queryExecutor.executeInTransaction(requests)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof LlrDatasourceException))
                                .completeNow()));
    }

    @Test
    void executeUpdate_Success(VertxTestContext testContext) {
        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.executeUpdate(sql)
                .onComplete(ar ->
                        testContext.verify(() ->
                                assertTrue(ar.succeeded()))
                                .completeNow());
    }

    @Test
    void executeUpdate_InvalidQuery_Fail(VertxTestContext testContext) {
        val sql = String.format(SELECT_ALL, INVALID_DB, TABLE);

        queryExecutor.executeUpdate(sql)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof PgException))
                                .completeNow()));
    }

    @Test
    void executeUpdate_InvalidConnection_Fail(Vertx vertx, VertxTestContext testContext) {
        val pgPool = getInvalidPgPool(vertx);
        queryExecutor = new AdpQueryExecutor(pgPool, 1000, fromSqlConverter, toSqlConverter);

        val sql = String.format(SELECT_ALL, DB_NAME, TABLE);

        queryExecutor.executeUpdate(sql)
                .onComplete(testContext.failing(error ->
                        testContext.verify(() ->
                                assertTrue(error instanceof UnknownHostException))
                                .completeNow()));
    }

    private Map<ColumnType, Object> expectedTypeValues() {
        Map<ColumnType, Object> typeValues = new HashMap<>();
        typeValues.put(ColumnType.VARCHAR, "varchar");
        typeValues.put(ColumnType.CHAR, "char");
        typeValues.put(ColumnType.BIGINT, 11L);
        typeValues.put(ColumnType.INT, 12L);
        typeValues.put(ColumnType.INT32, 13);
        typeValues.put(ColumnType.DOUBLE, 1.4);
        typeValues.put(ColumnType.FLOAT, 1.5F);
        typeValues.put(ColumnType.DATE, LocalDate.parse("2017-03-14"));
        typeValues.put(ColumnType.TIME, LocalTime.parse("00:00:00.000001"));
        typeValues.put(ColumnType.TIMESTAMP, LocalDateTime.parse("2016-06-22T19:10:25"));
        typeValues.put(ColumnType.BOOLEAN, true);
        typeValues.put(ColumnType.UUID, "uuid");
        typeValues.put(ColumnType.LINK, "link");
        return typeValues;
    }

    private PgPool getInvalidPgPool(Vertx vertx) {
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(container.getMappedPort(5432))
                .setHost("invalid_host")
                .setDatabase(DB_NAME)
                .setUser(USERNAME)
                .setPassword(PASSWORD);
        return PgPool.pool(vertx, connectOptions, new PoolOptions());
    }
}
