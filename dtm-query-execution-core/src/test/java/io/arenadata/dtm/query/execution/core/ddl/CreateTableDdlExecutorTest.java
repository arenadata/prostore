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

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.base.utils.InformationSchemaUtils;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.CreateTableDdlExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.MetadataCalciteGeneratorImpl;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.MetadataExecutorImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
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
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);
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
        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(schema, entity.getName()))
                .thenReturn(Future.succeededFuture(false));

        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());

        when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithInformationSchema() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        String schema = InformationSchemaUtils.INFORMATION_SCHEMA;
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("create table information_schema.accounts (id integer, name varchar(100))");
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);
        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(schema, entity.getName()))
                .thenReturn(Future.succeededFuture(false));

        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());

        when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("Creating tables in schema [information_schema] is not supported", promise.future().cause().getMessage());
    }

    @Test
    void executeWithDuplicationFieldsError() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("create table accounts (id integer, id integer)");
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "id", ColumnType.INT, false);
        String sqlNodeName = "accounts";
        entity = new Entity(sqlNodeName, schema, Arrays.asList(f1, f2));

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("Entity has duplication fields names", promise.future().cause().getMessage());
    }

    @Test
    void executeWithExistsDatamartError() {
        Promise<QueryResult> promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.failedFuture(new DatamartNotExistsException(schema)));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals(String.format("Database %s does not exist", schema), promise.future().cause().getMessage());
    }

    @Test
    void executeWithNotExistsDatamart() {
        Promise<QueryResult> promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(false));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals(String.format("Database %s does not exist", schema), promise.future().cause().getMessage());
    }

    @Test
    void executeWithTableExists() {
        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(schema, entity.getName()))
                .thenReturn(Future.succeededFuture(true));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals(String.format("Entity %s.%s already exists", schema, entity.getName()), promise.future().cause().getMessage());
    }

    @Test
    void executeWithTableExistsError() {
        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(schema, entity.getName()))
                .thenReturn(Future.failedFuture(new DtmException("exists entity error")));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("exists entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithMetadataDataSourceError() {
        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(schema, entity.getName()))
                .thenReturn(Future.succeededFuture(false));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("metadata executor error")));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("metadata executor error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithInsertTableError() {
        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(schema, entity.getName()))
                .thenReturn(Future.succeededFuture(false));

        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());

        when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new DtmException("create entity error")));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("create entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithMetadataGeneratorError() {
        Promise<QueryResult> promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any()))
                .thenThrow(new DtmException("metadata generator error"));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("metadata generator error", promise.future().cause().getMessage());
    }
}
