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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.rollback;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.AdbRollbackRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AdbRollbackServiceTest {

    private final RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory = mock(AdbRollbackRequestFactory.class);
    private final AdbQueryExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private AdbRollbackService adbRollbackService;
    private Entity entity;

    @BeforeEach
    void setUp() {
        adbRollbackService = new AdbRollbackService(rollbackRequestFactory, adbQueryExecutor);
        entity = Entity.builder()
            .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
            .externalTableFormat("avro")
            .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
            .externalTableLocationType(ExternalTableLocationType.KAFKA)
            .externalTableUploadMessageLimit(1000)
            .name("upload_table")
            .schema("test")
            .externalTableSchema("")
            .build();
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        RollbackRequest rollbackRequest = RollbackRequest.builder()
            .datamart("test")
            .sysCn(1L)
            .queryRequest(new QueryRequest())
            .destinationTable("test_table")
            .entity(entity)
            .build();
        RollbackRequestContext context = new RollbackRequestContext(new RequestMetrics(), rollbackRequest);
        AdbRollbackRequest sqlList = new AdbRollbackRequest(
            PreparedStatementRequest.onlySql("deleteFromActualSql"),
            PreparedStatementRequest.onlySql("deleteFromHistory"),
            PreparedStatementRequest.onlySql("truncateSql"),
            PreparedStatementRequest.onlySql("insertSql")
        );
        when(rollbackRequestFactory.create(any())).thenReturn(sqlList);
        Map<String, Integer> execCount = new HashMap<>();
        sqlList.getStatements().forEach(request -> execCount.put(request.getSql(), 0));
        List<Map<String, Object>> resultSet = new ArrayList<>();

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Map<String, Object>>>> handler = invocation.getArgument(2);
            final String sql = invocation.getArgument(0);
            execCount.put(sql, 1);
            handler.handle(Future.succeededFuture(resultSet));
            return null;
        }).when(adbQueryExecutor).execute(any(), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Map<String, Object>>>> handler = invocation.getArgument(1);
            final List<PreparedStatementRequest> requests = invocation.getArgument(0);
            requests.forEach(r -> execCount.put(r.getSql(), 1));
            handler.handle(Future.succeededFuture(resultSet));
            return null;
        }).when(adbQueryExecutor).executeInTransaction(any(), any());

        adbRollbackService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertEquals(execCount.get(sqlList.getStatements().get(0).getSql()), 1);
        assertEquals(execCount.get(sqlList.getStatements().get(1).getSql()), 1);
        assertEquals(execCount.get(sqlList.getStatements().get(2).getSql()), 1);
        assertEquals(execCount.get(sqlList.getStatements().get(3).getSql()), 1);
    }

    @Test
    void executeError() {
        Promise promise = Promise.promise();
        RollbackRequest rollbackRequest = RollbackRequest.builder()
            .datamart("test")
            .sysCn(1L)
            .queryRequest(new QueryRequest())
            .destinationTable("test_table")
            .entity(entity)
            .build();
        RollbackRequestContext context = new RollbackRequestContext(new RequestMetrics(), rollbackRequest);

        when(rollbackRequestFactory.create(any())).thenReturn(new AdbRollbackRequest(
            PreparedStatementRequest.onlySql("deleteFromActualSql"),
            PreparedStatementRequest.onlySql("deleteFromHistory"),
            PreparedStatementRequest.onlySql("truncateSql"),
            PreparedStatementRequest.onlySql("insertSql")
        ));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Map<String, Object>>>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(adbQueryExecutor).execute(any(), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Map<String, Object>>>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(adbQueryExecutor).executeInTransaction(any(), any());

        adbRollbackService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
}
