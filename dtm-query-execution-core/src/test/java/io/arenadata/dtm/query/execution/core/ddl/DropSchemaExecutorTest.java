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
package io.arenadata.dtm.query.execution.core.ddl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.cache.service.CaffeineCacheService;
import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheServiceImpl;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.eddl.DropDatabase;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.DropSchemaExecutor;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.arenadata.dtm.common.reader.InformationSchemaView.SCHEMA_NAME;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DropSchemaExecutorTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final CacheService<String, HotDelta> hotDeltaCacheService = mock(CaffeineCacheService.class);
    private final CacheService<String, OkDelta> okDeltaCacheService = mock(CaffeineCacheService.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final CacheService<EntityKey, Entity> entityCacheService = mock(CaffeineCacheService.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheServiceImpl.class);
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService = mock(CaffeineCacheService.class);
    private final DropDatabase mockDropNode = mock(DropDatabase.class);
    private final SqlIdentifier mockIdentifier = mock(SqlIdentifier.class);private DropSchemaExecutor dropSchemaExecutor;
    private DdlRequestContext context;
    private DdlRequestContext informationSchemaContext;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        dropSchemaExecutor = new DropSchemaExecutor(metadataExecutor,
                hotDeltaCacheService,
                okDeltaCacheService,
                entityCacheService,
                materializedViewCacheService,
                serviceDbFacade,
                evictQueryTemplateCacheService);
        doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
        prepareContext(false);
    }

    private void prepareContext(boolean isLogicalOnly) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);

        String dropSchemaSql = "drop database shares";
        if (isLogicalOnly) {
            dropSchemaSql += " logical_only";
        }
        queryRequest.setSql(dropSchemaSql);

        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);

        informationSchemaContext = new DdlRequestContext(null,
                new DatamartRequest(queryRequest),
                mockDropNode,
                null,
                null);
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(datamartDao.deleteDatamart(schema))
                .thenReturn(Future.succeededFuture());

        dropSchemaExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService).evictByDatamartName(schema);
        verify(materializedViewCacheService).forEach(any());
        verify(entityCacheService).removeIf(any());
        verify(hotDeltaCacheService).remove(anyString());
        verify(okDeltaCacheService).remove(anyString());
        verify(metadataExecutor).execute(context);
    }

    @Test
    void executeSuccessLogicalOnly() throws SqlParseException {
        prepareContext(true);

        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(datamartDao.deleteDatamart(schema))
                .thenReturn(Future.succeededFuture());

        dropSchemaExecutor.execute(context, null)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService).evictByDatamartName(schema);
        verify(materializedViewCacheService).forEach(any());
        verify(entityCacheService).removeIf(any());
        verify(hotDeltaCacheService).remove(anyString());
        verify(okDeltaCacheService).remove(anyString());
        verify(metadataExecutor, never()).execute(context);
    }

    @Test
    void executeWithDropSchemaError() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        dropSchemaExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verify(evictQueryTemplateCacheService).evictByDatamartName(any());
        verify(materializedViewCacheService).forEach(any());
        verify(entityCacheService).removeIf(any());
        verify(hotDeltaCacheService).remove(anyString());
        verify(okDeltaCacheService).remove(anyString());
    }

    @Test
    void executeWithDropDatamartError() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(datamartDao.deleteDatamart(schema))
                .thenReturn(Future.failedFuture("delete datamart error"));

        dropSchemaExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verify(evictQueryTemplateCacheService).evictByDatamartName(any());
        verify(materializedViewCacheService).forEach(any());
        verify(entityCacheService).removeIf(any());
        verify(hotDeltaCacheService).remove(anyString());
        verify(okDeltaCacheService).remove(anyString());
    }
}
