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
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.CreateTableDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.impl.MetadataCalciteGeneratorImpl;
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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CreateTableDdlExecutorTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataCalciteGenerator metadataCalciteGenerator = mock(MetadataCalciteGeneratorImpl.class);
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private QueryResultDdlExecutor createTableDdlExecutor;
    private DdlRequestContext context;
    private Entity entity;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        Set<SourceType> sourceTypes = new HashSet<>();
        sourceTypes.add(SourceType.ADB);
        sourceTypes.add(SourceType.ADG);
        sourceTypes.add(SourceType.ADQM);
        when(dataSourcePluginService.getSourceTypes()).thenReturn(sourceTypes);
        when(dataSourcePluginService.getSourceTypes()).thenReturn(sourceTypes);
        createTableDdlExecutor = new CreateTableDdlExecutor(metadataExecutor, serviceDbFacade, metadataCalciteGenerator,
                dataSourcePluginService);

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("create table accounts (id integer, name varchar(100))");
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
        entity = new Entity(sqlNodeName, schema, Arrays.asList(f1, f2));
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.succeededFuture(false));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.when(entityDao.createEntity(any()))
            .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithExistsDatamartError() {
        Promise promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.failedFuture(new DatamartNotExistsException(schema)));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithNotExistsDatamart() {
        Promise promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(false));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithTableExists() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);


        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.succeededFuture(true));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithTableExistsError() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.failedFuture("exists entity error"));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithMetadataDataSourceError() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.succeededFuture(false));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(metadataExecutor).execute(any(), any());

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithInsertTableError() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);


        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.succeededFuture(false));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.when(entityDao.createEntity(any()))
            .thenReturn(Future.failedFuture("create entity error"));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithMetadataGeneratorError() {
        Promise promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenThrow(new RuntimeException());

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}
