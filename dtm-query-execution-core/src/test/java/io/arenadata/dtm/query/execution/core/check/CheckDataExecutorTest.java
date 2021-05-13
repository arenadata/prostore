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
package io.arenadata.dtm.query.execution.core.check;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckData;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.check.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.core.check.factory.impl.CheckQueryResultFactoryImpl;
import io.arenadata.dtm.query.execution.core.check.service.impl.CheckDataExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.base.verticle.TaskVerticleExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class CheckDataExecutorTest {
    private final static String DATAMART_MNEMONIC = "schema";
    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDao.class);
    private final TaskVerticleExecutor taskVerticleExecutor = mock(TaskVerticleExecutor.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final CheckQueryResultFactory queryResultFactory = mock(CheckQueryResultFactoryImpl.class);
    private final CheckDataExecutor checkDataExecutor = new CheckDataExecutor(dataSourcePluginService,
            deltaServiceDao, entityDao, taskVerticleExecutor, queryResultFactory);
    private Entity entity;

    @BeforeEach
    void setUp() {
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(dataSourcePluginService.checkDataByCount(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(datamartDao.getDatamart(DATAMART_MNEMONIC)).thenReturn(Future.succeededFuture(new byte[10]));
        doAnswer(invocation -> {
            Handler<Promise> codeHandler = invocation.getArgument(0);
            codeHandler.handle(Promise.promise());
            Handler<AsyncResult> resultHandler = invocation.getArgument(1);
            resultHandler.handle(Future.succeededFuture());
            return null;
        }).when(taskVerticleExecutor).execute(any(), any());

        OkDelta okDelta = mock(OkDelta.class);
        when(okDelta.getCnFrom()).thenReturn(0L);
        when(okDelta.getCnTo()).thenReturn(1L);
        when(deltaServiceDao.getDeltaOk(eq(DATAMART_MNEMONIC))).thenReturn(Future.succeededFuture(okDelta));
        when(deltaServiceDao.getDeltaByNum(eq(DATAMART_MNEMONIC), anyLong())).thenReturn(Future.succeededFuture(okDelta));

        entity = Entity.builder()
                .schema(DATAMART_MNEMONIC)
                .entityType(EntityType.TABLE)
                .destination(SOURCE_TYPES)
                .name("entity")
                .fields(Collections.singletonList(EntityField.builder()
                        .name("field")
                        .build()))
                .build();
        when(entityDao.getEntity(DATAMART_MNEMONIC, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
    }

    @Test
    void testCheckByHashSuccess() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        SqlCheckData sqlCheckData = mock(SqlCheckData.class);
        when(sqlCheckData.getColumns()).thenReturn(Stream.of("field").collect(Collectors.toSet()));
        when(sqlCheckData.getDeltaNum()).thenReturn(0L);
        when(sqlCheckData.getTable()).thenReturn(entity.getName());
        when(sqlCheckData.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.DATA, sqlCheckData);
        checkDataExecutor.execute(checkContext)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    SOURCE_TYPES.forEach(sourceType ->
                            verify(dataSourcePluginService, times(2))
                                    .checkDataByCount(eq(sourceType), any(),
                                            argThat(request -> request.getEntity().getName().equals(entity.getName()))));
                });

    }

    @Test
    void testCheckByCountSuccess() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        SqlCheckData sqlCheckData = mock(SqlCheckData.class);
        when(sqlCheckData.getDeltaNum()).thenReturn(0L);
        when(sqlCheckData.getTable()).thenReturn(entity.getName());
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.DATA, sqlCheckData);
        checkDataExecutor.execute(checkContext)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    SOURCE_TYPES.forEach(sourceType ->
                            verify(dataSourcePluginService, times(2))
                                    .checkDataByHashInt32(eq(sourceType), any(),
                                            argThat(request -> request.getEntity().getName().equals(entity.getName()))));
                });

    }
}
