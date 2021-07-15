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
import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlType;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.DropTableOrMaterializedDdlExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DropTableOrMaterializedDdlExecutorTest {
    private static final String SCHEMA = "shares";

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();

    @Mock
    private MetadataExecutor<DdlRequestContext> metadataExecutor;
    @Mock
    private DataSourcePluginService pluginService;
    @Mock
    private CacheService<EntityKey, Entity> cacheService;
    @Mock
    private CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;
    @Mock
    private DatamartDao datamartDao;
    @Mock
    private EntityDao entityDao;
    @Mock
    private EvictQueryTemplateCacheService evictQueryTemplateCacheService;
    @Mock
    private HSQLClient hsqlClient;

    @Captor
    private ArgumentCaptor<DdlRequestContext> contextArgumentCaptor;

    private QueryResultDdlExecutor dropTableDdlExecutor;
    private DdlRequestContext context;

    @BeforeEach
    void setUp() {
        lenient().when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        lenient().when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        lenient().when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        dropTableDdlExecutor = new DropTableOrMaterializedDdlExecutor(cacheService, metadataExecutor, serviceDbFacade, pluginService, materializedViewCacheService, hsqlClient,
                evictQueryTemplateCacheService);
        lenient().doNothing().when(evictQueryTemplateCacheService).evictByEntityName(anyString(), anyString());
    }

    @Test
    void executeSuccessTable() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts", EntityType.TABLE);
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture());

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        verifyNoInteractions(materializedViewCacheService);
        DdlRequestContext value = contextArgumentCaptor.getValue();
        assertSame(DdlType.DROP_TABLE, value.getDdlType());
        assertNull(value.getSourceType());
    }

    @Test
    void executeSuccessMaterializedView() throws SqlParseException {
        // arrange
        prepareContext("drop materialized view accounts", EntityType.MATERIALIZED_VIEW);

        Entity entity = context.getEntity();


        Promise<QueryResult> promise = Promise.promise();

        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture());

        when(materializedViewCacheService.get(any())).thenReturn(new MaterializedViewCacheValue(entity));

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        verify(materializedViewCacheService).get(any());
        DdlRequestContext value = contextArgumentCaptor.getValue();
        assertSame(DdlType.DROP_MATERIALIZED_VIEW, value.getDdlType());
        assertNull(value.getSourceType());
    }

    @Test
    void executeFailedWhenDroppingTableAndMaterializedView() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts", EntityType.TABLE);
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        entity.setEntityType(EntityType.MATERIALIZED_VIEW);
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().failed());
        TestUtils.assertException(EntityNotExistsException.class, "does not exist", promise.future().cause());
        verifyNoInteractions(pluginService, hsqlClient, metadataExecutor, materializedViewCacheService);
    }

    @Test
    void executeWithFindView() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts", EntityType.TABLE);
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(Entity.builder()
                        .schema(SCHEMA)
                        .name(entity.getName())
                        .entityType(EntityType.VIEW)
                        .build()));

        // act
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, never())
                .evictByEntityName(entity.getSchema(), entity.getName());
        verifyNoInteractions(pluginService, hsqlClient, materializedViewCacheService);
    }

    @Test
    void executeWithIfExistsStmtSuccess() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts", EntityType.TABLE);
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        context.getRequest().getQueryRequest().setSql("DROP TABLE IF EXISTS accounts");
        String entityName = entity.getName();
        when(entityDao.getEntity(SCHEMA, entityName))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityName)));

        // act
        dropTableDdlExecutor.execute(context, entityName)
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().result());
        verify(evictQueryTemplateCacheService, never())
                .evictByEntityName(entity.getSchema(), entity.getName());
        verifyNoInteractions(pluginService, hsqlClient, materializedViewCacheService);
    }

    @Test
    void executeWithMetadataExecuteError() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts", EntityType.TABLE);
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("Error drop table in plugin")));

        // act
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
        verifyNoInteractions(materializedViewCacheService);
    }

    @Test
    void executeWithDropEntityError() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts", EntityType.TABLE);
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.failedFuture(new DtmException("delete entity error")));

        // act
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
        verifyNoInteractions(materializedViewCacheService);
    }

    @Test
    void executeWithExistedViewError() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts", EntityType.TABLE);
        Promise<QueryResult> promise = Promise.promise();
        String viewName = "VIEWNAME";
        String expectedMessage = String.format("View ‘%s’ using the '%s' must be dropped first", viewName, context.getEntity().getName().toUpperCase());

        when(entityDao.getEntity(SCHEMA, context.getEntity().getName()))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.singletonList(new JsonArray().add(viewName)))));

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertEquals(expectedMessage, promise.future().cause().getMessage());
        verifyNoInteractions(pluginService, materializedViewCacheService);
    }

    @Test
    void executeCorrectlyExtractSourceTypeInMaterializedView() throws SqlParseException {
        // arrange
        prepareContext("drop materialized view accounts datasource_type = 'ADB'", EntityType.MATERIALIZED_VIEW);
        Entity entity = context.getEntity();
        Promise<QueryResult> promise = Promise.promise();

        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture());

        when(materializedViewCacheService.get(any())).thenReturn(new MaterializedViewCacheValue(entity));

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        verify(materializedViewCacheService).get(any());
        DdlRequestContext value = contextArgumentCaptor.getValue();
        assertSame(DdlType.DROP_MATERIALIZED_VIEW, value.getDdlType());
        assertSame(SourceType.ADB, value.getSourceType());
    }

    @Test
    void executeCorrectlyExtractSourceTypeInTable() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts datasource_type = 'ADB'", EntityType.TABLE);
        Entity entity = context.getEntity();

        Promise<QueryResult> promise = Promise.promise();

        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture());

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        verifyNoInteractions(materializedViewCacheService);
        DdlRequestContext value = contextArgumentCaptor.getValue();
        assertSame(DdlType.DROP_TABLE, value.getDdlType());
        assertSame(SourceType.ADB, value.getSourceType());
    }

    private void prepareContext(String s, EntityType materializedView) throws SqlParseException {
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(SCHEMA);
        queryRequest.setSql(s);
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(100);
        String sqlNodeName = "accounts";
        Entity ctxEntity = new Entity(sqlNodeName, SCHEMA, Arrays.asList(f1, f2));
        ctxEntity.setEntityType(materializedView);
        ctxEntity.setDestination(Collections.singleton(SourceType.ADB));
        context.setEntity(ctxEntity);
        context.setDatamartName(SCHEMA);
    }
}
