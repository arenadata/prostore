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
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlAlterView;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.AlterViewExecutor;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.arenadata.dtm.query.execution.core.utils.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AlterViewExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final LogicalSchemaProvider logicalSchemaProvider = mock(LogicalSchemaProviderImpl.class);
    private final ColumnMetadataService columnMetadataService = mock(ColumnMetadataService.class);
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final CacheService<EntityKey, Entity> entityCacheService = mock(CaffeineCacheService.class);
    private final QueryParserService parserService = mock(CoreCalciteDMLQueryParserService.class);

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
            .configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final CoreCalciteSchemaFactory coreSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CalciteContextProvider contextProvider = new CoreCalciteContextProvider(parserConfig, coreSchemaFactory);
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(SQL_DIALECT);

    private AlterViewExecutor alterViewExecutor;
    private String sqlNodeName;
    private String schema;
    private final List<Entity> entityList = new ArrayList<>();
    private List<Datamart> logicSchema;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);

        alterViewExecutor = new AlterViewExecutor(entityCacheService,
                metadataExecutor,
                logicalSchemaProvider,
                columnMetadataService,
                serviceDbFacade,
                new SqlDialect(SqlDialect.EMPTY_CONTEXT),
                parserService,
                relToSqlConverter);
        schema = "shares";
        initEntityList(entityList, schema);
        logicSchema = Collections.singletonList(new Datamart(
                schema,
                true,
                entityList));
        sqlNodeName = schema + "." + entityList.get(0).getName();
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(logicalSchemaProvider.getSchemaFromQuery(any(), any()))
                .thenReturn(Future.succeededFuture(logicSchema));
    }

    @Test
    void executeSuccess() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(0).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.updateEntity(any()))
                .thenReturn(Future.succeededFuture());

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeWrongEntityTypeError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(1).getName(), schema, entityList.get(2).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(2).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals(String.format("Entity %s does not exist", entityList.get(1).getName()), promise.future().cause().getMessage());
    }

    @Test
    void executeSuccessWithJoinQuery() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();
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

        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(2).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(3).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(3)));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(0).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.updateEntity(any()))
                .thenReturn(Future.succeededFuture());

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeIsEntityExistsError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(0).getName())))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityList.get(0).getName())));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithViewUpdateError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(0).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.updateEntity(any()))
                .thenReturn(Future.failedFuture(new DtmException("Update error")));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeQueryContainsViewError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(1).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeSuccessColumnDuplication() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * " +
                        "FROM %s.%s a " +
                        "JOIN %s.%s t on t.id = a.id",
                schema, entityList.get(0).getName(),
                schema, entityList.get(4).getName(),
                schema, entityList.get(5).getName()));

        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(4).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(4)));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(5).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(5)));

        when(entityDao.getEntity(eq(schema), eq(entityList.get(0).getName())))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.updateEntity(any()))
                .thenReturn(Future.succeededFuture());

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeWithTimestampSuccess() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s WHERE timestamp_col = '2020-12-01 00:00:00'",
                schema, entityList.get(0).getName(), schema, entityList.get(6).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Arrays.asList(ColumnMetadata.builder()
                                .name("id")
                                .type(ColumnType.BIGINT)
                                .build(),
                        ColumnMetadata.builder()
                                .name("timestamp_col")
                                .type(ColumnType.TIMESTAMP)
                                .build())));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.getEntity(schema, entityList.get(6).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(6)));

        when(entityDao.updateEntity(any()))
                .thenReturn(Future.succeededFuture());

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeWrongTimestampFormatError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);

        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s WHERE timestamp_col = '123456'",
                schema, entityList.get(0).getName(), schema, entityList.get(6).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Arrays.asList(ColumnMetadata.builder()
                                .name("id")
                                .type(ColumnType.BIGINT)
                                .build(),
                        ColumnMetadata.builder()
                                .name("timestamp_col")
                                .type(ColumnType.TIMESTAMP)
                                .build())));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.getEntity(schema, entityList.get(6).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(6)));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof ValidationDtmException);
    }
}
