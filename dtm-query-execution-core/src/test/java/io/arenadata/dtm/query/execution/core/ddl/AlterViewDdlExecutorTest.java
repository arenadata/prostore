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
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlAlterView;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.AlterViewDdlExecutor;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    private static final LimitSqlDialect SQL_DIALECT = new LimitSqlDialect(CalciteSqlDialect.DEFAULT_CONTEXT);

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

    private AlterViewDdlExecutor alterViewDdlExecutor;
    private String sqlNodeName;
    private String schema;
    private final List<Entity> entityList = new ArrayList<>();
    private List<Datamart> logicSchema;

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
                new SqlDialect(SqlDialect.EMPTY_CONTEXT),
                parserService);
        schema = "shares";
        initEntityList();
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
                .thenReturn(Future.succeededFuture(parse(new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

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

        alterViewDdlExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
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
                .thenReturn(Future.succeededFuture(parse(new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

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

        alterViewDdlExecutor.execute(context, sqlNodeName)
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

        alterViewDdlExecutor.execute(context, sqlNodeName)
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

        alterViewDdlExecutor.execute(context, sqlNodeName)
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

        alterViewDdlExecutor.execute(context, sqlNodeName)
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
                .thenReturn(Future.succeededFuture(parse(new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

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

        alterViewDdlExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
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

        List<EntityField> accountFields = new ArrayList<>();
        accountFields.add(EntityField.builder()
                .ordinalPosition(0)
                .name("id")
                .type(ColumnType.BIGINT)
                .nullable(false)
                .build());
        accountFields.add(EntityField.builder()
                .ordinalPosition(0)
                .name("account_type")
                .type(ColumnType.BIGINT)
                .nullable(false)
                .build());
        Entity entity5 = Entity.builder()
                .schema(schema)
                .name("accounts1")
                .fields(accountFields)
                .entityType(EntityType.TABLE)
                .build();
        Entity entity6 = Entity.builder()
                .schema(schema)
                .name("accounts2")
                .fields(accountFields)
                .entityType(EntityType.TABLE)
                .build();

        entityList.add(entity1);
        entityList.add(entity2);
        entityList.add(entity3);
        entityList.add(entity4);
        entityList.add(entity5);
        entityList.add(entity6);
    }

    @SneakyThrows
    private QueryParserResponse parse(QueryParserRequest request) {
            val context = contextProvider.context(request.getSchema());
            val sql = request.getQuery().toSqlString(SQL_DIALECT).getSql();
            val parse = context.getPlanner().parse(sql);
            val validatedQuery = context.getPlanner().validate(parse);
            val relQuery = context.getPlanner().rel(validatedQuery);
            return new QueryParserResponse(
                    context,
                    request.getSchema(),
                    relQuery,
                    validatedQuery);
    }
}
