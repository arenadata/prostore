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
package io.arenadata.dtm.query.execution.core.ddl.table;

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
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ChangelogDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.MetadataCalciteGeneratorImpl;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.table.CreateTableExecutor;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
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

import static io.arenadata.dtm.query.execution.core.utils.TestUtils.SQL_DIALECT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CreateTableExecutorTest {

    private static final String DATAMART = "shares";
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataCalciteGenerator metadataCalciteGenerator = mock(MetadataCalciteGeneratorImpl.class);
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ChangelogDao changelogDao = mock(ChangelogDao.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDao.class);
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final OkDelta deltaOk = OkDelta.builder()
            .deltaNum(1)
            .build();
    private QueryResultDdlExecutor createTableDdlExecutor;
    private DdlRequestContext context;
    private Entity entity;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbDao.getChangelogDao()).thenReturn(changelogDao);
        Set<SourceType> sourceTypes = new HashSet<>();
        sourceTypes.add(SourceType.ADB);
        sourceTypes.add(SourceType.ADG);
        sourceTypes.add(SourceType.ADQM);
        when(dataSourcePluginService.getSourceTypes()).thenReturn(sourceTypes);
        when(dataSourcePluginService.getSourceTypes()).thenReturn(sourceTypes);
        createTableDdlExecutor = new CreateTableExecutor(metadataExecutor, serviceDbFacade, SQL_DIALECT, metadataCalciteGenerator, dataSourcePluginService);

        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(100);
        String sqlNodeName = "accounts";
        entity = new Entity(sqlNodeName, DATAMART, Arrays.asList(f1, f2));
    }

    private void prepareContext(String sql, boolean isLogicalOnly) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(DATAMART);

        if (isLogicalOnly) {
            sql += " logical_only";
        }
        queryRequest.setSql(sql);

        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(DATAMART);
    }

    @Test
    void executeSuccess() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().result());

        verify(datamartDao).existsDatamart(DATAMART);
        verify(entityDao).setEntityState(any(), any(), eq("CREATE TABLE shares.accounts (id INTEGER, name VARCHAR(100))"), any());
        verify(entityDao).existsEntity(DATAMART, entity.getName());
        verify(metadataExecutor).execute(context);
    }

    @Test
    void executeSuccessWithoutDeltaOk() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(null));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().result());

        verify(datamartDao).existsDatamart(DATAMART);
        verify(entityDao).existsEntity(DATAMART, entity.getName());
        verify(entityDao).setEntityState(any(), eq(null), eq("CREATE TABLE shares.accounts (id INTEGER, name VARCHAR(100))"), any());
        verify(metadataExecutor).execute(context);
    }

    @Test
    void executeGetDeltaOkError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.failedFuture("get delta ok error"));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("get delta ok error", promise.future().cause().getMessage());
    }

    @Test
    void executeSuccessLogicalOnly() throws SqlParseException {
        prepareContext("create table accounts (id integer, name varchar(100))", true);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().result());

        verify(datamartDao).existsDatamart(DATAMART);
        verify(entityDao).existsEntity(DATAMART, entity.getName());
        verify(entityDao).setEntityState(any(), eq(deltaOk), eq("CREATE TABLE accounts (id INTEGER, name VARCHAR(100)) LOGICAL_ONLY"), any());
        verify(metadataExecutor, never()).execute(context);
    }

    @Test
    void executeWithInvalidShardingKeyError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);
        entity.getFields().get(0).setShardingOrder(null);
        entity.getFields().get(1).setShardingOrder(1);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));

        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("DISTRIBUTED BY clause must be a subset of the PRIMARY KEY", promise.future().cause().getMessage());
    }

    @Test
    void executeWithDuplicationFieldsError() throws SqlParseException {
        prepareContext("create table accounts (id integer, id integer)", false);

        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "id", ColumnType.INT, false);
        String sqlNodeName = "accounts";
        entity = new Entity(sqlNodeName, DATAMART, Arrays.asList(f1, f2));

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("Entity has duplication fields names", promise.future().cause().getMessage());
    }

    @Test
    void executeWithExistsDatamartError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.failedFuture(new DatamartNotExistsException(DATAMART)));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals(String.format("Database %s does not exist", DATAMART), promise.future().cause().getMessage());
    }

    @Test
    void executeWithNotExistsDatamartError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(false));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals(String.format("Database %s does not exist", DATAMART), promise.future().cause().getMessage());
    }

    @Test
    void executeWithTableExists() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(true));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals(String.format("Entity %s.%s already exists", DATAMART, entity.getName()), promise.future().cause().getMessage());
    }

    @Test
    void executeWithTableExistsError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.failedFuture(new DtmException("exists entity error")));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("exists entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeWriteChangelogError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(DATAMART, entity.getName(), "CREATE TABLE shares.accounts (id INTEGER, name VARCHAR(100))", deltaOk))
                .thenReturn(Future.failedFuture("write new changelog record error"));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("write new changelog record error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithMetadataDataSourceError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));

        when(deltaServiceDao.getDeltaOk(DATAMART))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("metadata executor error")));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("metadata executor error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithInsertTableError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), anyString(), any()))
                .thenReturn(Future.failedFuture(new DtmException("create entity error")));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("create entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithMetadataGeneratorError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any()))
                .thenThrow(new DtmException("metadata generator error"));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("metadata generator error", promise.future().cause().getMessage());
    }
}
