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
package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.DropTableDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DropTableDdlExecutorTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private final EntityCacheService cacheService = mock(EntityCacheService.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private QueryResultDdlExecutor dropTableDdlExecutor;
    private DdlRequestContext context;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        dropTableDdlExecutor = new DropTableDdlExecutor(cacheService, metadataExecutor, serviceDbFacade, pluginService);

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("drop table accounts");
        SqlNode query = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(100);
        String sqlNodeName = "accounts";
        Entity entity = new Entity(sqlNodeName, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.TABLE);
        entity.setDestination(Collections.singleton(SourceType.ADB));
        context.getRequest().setEntity(entity);
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();

        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));

        Mockito.when(entityDao.getEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture(context.getRequest().getEntity()));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture());

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
            assertNotNull(promise.future().result());
        });
    }

    @Test
    void executeWithFindView() {
        Promise promise = Promise.promise();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));

        Mockito.when(entityDao.getEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture(Entity.builder()
                .schema(schema)
                .name(context.getRequest().getEntity().getName())
                .entityType(EntityType.VIEW)
                .build()));

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithIfExistsStmtSuccess() {
        Promise promise = Promise.promise();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));
        context.getRequest().getQueryRequest().setSql("DROP TABLE IF EXISTS accounts");
        String entityName = context.getRequest().getEntity().getName();
        Mockito.when(entityDao.getEntity(eq(schema), eq(entityName)))
            .thenReturn(Future.failedFuture(new EntityNotExistsException(entityName)));

        dropTableDdlExecutor.execute(context, entityName, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithMetadataExecuteError() {
        Promise promise = Promise.promise();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));
        Mockito.when(entityDao.getEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture(context.getRequest().getEntity()));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException()));
            return null;
        }).when(metadataExecutor).execute(any(), any());

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithDropEntityError() {
        Promise promise = Promise.promise();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));
        Mockito.when(entityDao.getEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture(context.getRequest().getEntity()));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.failedFuture("delete entity error"));

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}
