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
package io.arenadata.dtm.query.execution.core.edml;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.edml.SqlRollbackCrashedWriteOps;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.dto.EraseWriteOpResult;
import io.arenadata.dtm.query.execution.core.rollback.factory.RollbackWriteOpsQueryResultFactory;
import io.arenadata.dtm.query.execution.core.rollback.factory.impl.RollbackWriteOpsQueryResultFactoryImpl;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.RollbackCrashedWriteOpExecutor;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.arenadata.dtm.query.execution.core.rollback.factory.impl.RollbackWriteOpsQueryResultFactoryImpl.SYS_CN_OPS_COLUMN;
import static io.arenadata.dtm.query.execution.core.rollback.factory.impl.RollbackWriteOpsQueryResultFactoryImpl.TABLE_NAME_COLUMN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RollbackCrashedWriteOpExecutorTest {

    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private final RestoreStateService restoreStateService = mock(RestoreStateService.class);
    private RollbackCrashedWriteOpExecutor rollbackCrashedWriteOpExecutor;
    private RollbackWriteOpsQueryResultFactory writeOpsQueryResultFactory;
    private QueryRequest queryRequest;
    private EdmlRequestContext context;

    @BeforeEach
    void setUp() {
        rollbackCrashedWriteOpExecutor = new RollbackCrashedWriteOpExecutor(restoreStateService,
                new RollbackWriteOpsQueryResultFactoryImpl());
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        List<EraseWriteOpResult> eraseOpList = new ArrayList<>();
        eraseOpList.addAll(Arrays.asList(
                new EraseWriteOpResult("t1", 1),
                new EraseWriteOpResult("t1", 2),
                new EraseWriteOpResult("t1", 3),
                new EraseWriteOpResult("t2", 7),
                new EraseWriteOpResult("t3", 7)
                ));
        queryRequest.setSql("ROLLBACK CRASHED_WRITE_OPERATIONS");
        SqlRollbackCrashedWriteOps sqlNode = (SqlRollbackCrashedWriteOps) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "test");

        when(restoreStateService.restoreErase(any())).thenReturn(Future.succeededFuture(eraseOpList));

        rollbackCrashedWriteOpExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        QueryResult result = promise.future().result();
        assertEquals(3, result.getResult().size());
        assertEquals("t1", result.getResult().get(0).get(TABLE_NAME_COLUMN));
        assertEquals("1, 2, 3", result.getResult().get(0).get(SYS_CN_OPS_COLUMN));
    }

    @Test
    void executeEmptyResultSuccess() {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql("ROLLBACK CRASHED_WRITE_OPERATIONS");
        SqlRollbackCrashedWriteOps sqlNode = (SqlRollbackCrashedWriteOps) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "test");

        when(restoreStateService.restoreErase(any())).thenReturn(Future.succeededFuture(Collections.emptyList()));

        rollbackCrashedWriteOpExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        QueryResult result = promise.future().result();
        assertTrue(result.getResult().isEmpty());
    }

    @Test
    void executeRestoreError() {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql("ROLLBACK CRASHED_WRITE_OPERATIONS");
        SqlRollbackCrashedWriteOps sqlNode = (SqlRollbackCrashedWriteOps) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "test");

        when(restoreStateService.restoreErase(any())).thenReturn(Future.failedFuture(new DtmException("")));

        rollbackCrashedWriteOpExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }
}