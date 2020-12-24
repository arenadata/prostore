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
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.exception.entity.ViewNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.AlterViewDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.service.schema.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AlterViewDdlExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final LogicalSchemaProvider logicalSchemaProvider = mock(LogicalSchemaProviderImpl.class);
    private final ColumnMetadataService columnMetadataService = mock(ColumnMetadataService.class);
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final EntityCacheService entityCacheService = mock(EntityCacheService.class);
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
        .configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private AlterViewDdlExecutor alterViewDdlExecutor;
    private String sqlNodeName;
    private String schema;
    private List<Entity> entityList = new ArrayList<>();

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);

        alterViewDdlExecutor = new AlterViewDdlExecutor(entityCacheService,
            metadataExecutor,
            logicalSchemaProvider,
            columnMetadataService,
            serviceDbFacade,
            new SqlDialect(SqlDialect.EMPTY_CONTEXT));
        schema = "shares";

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Datamart>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(
                Collections.singletonList(new Datamart(
                    schema,
                    true,
                    Collections.singletonList(entityList.get(0))
                ))
            ));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());
        initEntityList();
        sqlNodeName = schema + "." + entityList.get(0).getName();
    }

    @Test
    void executeSuccess() throws SqlParseException {
        Promise promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
            schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode query = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<ColumnMetadata>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .build())));
            return null;
        }).when(columnMetadataService).getColumnMetadata(any(), any());

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(1)));

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(0).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(0)));

        Mockito.when(entityDao.updateEntity(any()))
            .thenReturn(Future.succeededFuture());

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            }
        );
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeSuccessWithJoinQuery() throws SqlParseException {
        Promise promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * " +
                "FROM (select a.id FROM %s a " +
                "JOIN %s.%s t on t.id = a.id)",
            schema, entityList.get(0).getName(), entityList.get(2).getName(),
            schema, entityList.get(3).getName()));

        SqlNode query = planner.parse(queryRequest.getSql());

        DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<ColumnMetadata>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .build())));
            return null;
        }).when(columnMetadataService).getColumnMetadata(any(), any());

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(2).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(2)));

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(3).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(3)));

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(0).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(0)));

        Mockito.when(entityDao.updateEntity(any()))
            .thenReturn(Future.succeededFuture());

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            }
        );
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeIsEntityExistsError() throws SqlParseException {
        Promise promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
            schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode query = planner.parse(queryRequest.getSql());

        DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);
        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(1)));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<ColumnMetadata>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .build())));
            return null;
        }).when(columnMetadataService).getColumnMetadata(any(), any());

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(0).getName())))
            .thenReturn(Future.failedFuture(new ViewNotExistsException(entityList.get(0).getName())));

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            }
        );
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithViewUpdateError() throws SqlParseException {
        Promise promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
            schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode query = planner.parse(queryRequest.getSql());

        DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(1)));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<ColumnMetadata>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .build())));
            return null;
        }).when(columnMetadataService).getColumnMetadata(any(), any());

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(0).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(0)));

        Mockito.when(entityDao.updateEntity(any()))
            .thenReturn(Future.failedFuture("Update error"));

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            }
        );
        assertTrue(promise.future().failed());
    }

    @Test
    void executeQueryContainsViewError() throws SqlParseException {
        Promise promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
            schema, entityList.get(0).getName(), schema, entityList.get(0).getName()));
        SqlNode query = planner.parse(queryRequest.getSql());

        DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<ColumnMetadata>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .build())));
            return null;
        }).when(columnMetadataService).getColumnMetadata(any(), any());

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(1)));

        Mockito.when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
            .thenReturn(Future.succeededFuture(entityList.get(1)));

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            }
        );
        assertTrue(promise.future().failed());
    }

    private void initEntityList() {
        List<EntityField> fields = Collections.singletonList(
            EntityField.builder()
                .ordinalPosition(0)
                .name("id")
                .type(ColumnType.BIGINT)
                .nullable(false)
                .build());
        Entity entity1 = Entity.builder()
            .schema(schema)
            .name("test_view")
            .viewQuery(String.format("SELECT * FROM %s.%s", schema, "test_table"))
            .fields(fields)
            .entityType(EntityType.VIEW)
            .build();
        Entity entity2 = Entity.builder()
            .schema(schema)
            .name("test_table")
            .fields(fields)
            .entityType(EntityType.TABLE)
            .build();
        Entity entity3 = Entity.builder()
            .schema(schema)
            .name("accounts")
            .fields(fields)
            .entityType(EntityType.TABLE)
            .build();
        Entity entity4 = Entity.builder()
            .schema(schema)
            .name("transactions")
            .fields(fields)
            .entityType(EntityType.TABLE)
            .build();
        entityList.add(entity1);
        entityList.add(entity2);
        entityList.add(entity3);
        entityList.add(entity4);
    }
}
