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
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateView;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
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
import io.arenadata.dtm.query.execution.core.ddl.service.impl.CreateViewExecutor;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CreateViewExecutorTest {

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
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(SQL_DIALECT);
    private final List<Entity> entityList = new ArrayList<>();

    private Planner planner;
    private CreateViewExecutor createViewExecutor;
    private String sqlNodeName;
    private String schema;
    private List<Datamart> logicSchema;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);

        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        createViewExecutor = new CreateViewExecutor(entityCacheService,
                metadataExecutor,
                logicalSchemaProvider,
                columnMetadataService,
                serviceDbFacade,
                new SqlDialect(SqlDialect.EMPTY_CONTEXT),
                parserService,
                relToSqlConverter);
        schema = "shares";
        queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
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

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeReplaceSuccess() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("")));

        when(entityDao.updateEntity(any()))
                .thenReturn(Future.succeededFuture());

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeCreateEntityError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new DtmException("create entity error")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("create entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeInvalidViewError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeEntityAlreadyExistError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("entity already exist")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof EntityAlreadyExistsException);
    }

    @Test
    void executeReplaceWrongEntityTypeError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(1).getName(), schema, entityList.get(2).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals(String.format("Entity %s does not exist", entityList.get(1).getName()), promise.future().cause().getMessage());
    }

    @Test
    void executeInnerJoinWrongEntityTypeError() throws SqlParseException {
        testJoinWithWrongEntityType("INNER");
    }

    @Test
    void executeFullJoinWrongEntityTypeError() throws SqlParseException {
        testJoinWithWrongEntityType("FULL");
    }

    @Test
    void executeLeftJoinWrongEntityTypeError() throws SqlParseException {
        testJoinWithWrongEntityType("LEFT");
    }

    @Test
    void executeRightJoinWrongEntityTypeError() throws SqlParseException {
        testJoinWithWrongEntityType("RIGHT");
    }

    @Test
    void executeCrossJoinWrongEntityTypeError() throws SqlParseException {
        testJoinWithWrongEntityType("CROSS");
    }

    @Test
    void executeMultipleJoinWrongEntityTypeError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s " +
                        "JOIN %s.%s ON %s.id = %s.id " +
                        "JOIN %s.%s ON %s.id = %s.id",
                schema, entityList.get(1).getName(), schema, entityList.get(2).getName(),
                schema, entityList.get(3).getName(), entityList.get(2).getName(), entityList.get(3).getName(),
                schema, entityList.get(0).getName(), entityList.get(2).getName(), entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.getEntity(schema, entityList.get(3).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(3)));

        when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertThat(promise.future().cause().getMessage()).startsWith("Disallowed view or directive in a subquery");
    }

    @Test
    void executeWithTimestampSuccess() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s WHERE timestamp_col = '2020-12-01 00:00:00'",
                schema, entityList.get(0).getName(), schema, entityList.get(6).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

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

        when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeWrongTimestampFormatError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s WHERE timestamp_col = '123456'",
                schema, entityList.get(0).getName(), schema, entityList.get(6).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

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

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof ValidationDtmException);
    }

    private void testJoinWithWrongEntityType(String joinType) throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        String sql = String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s " +
                        "%s JOIN %s.%s",
                schema, entityList.get(1).getName(), schema, entityList.get(2).getName(),
                joinType, schema, entityList.get(0).getName());
        if (!joinType.equals("CROSS")) {
            sql += String.format(" ON %s.id = %s.id", entityList.get(2).getName(), entityList.get(0).getName());
        }
        queryRequest.setSql(sql);
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertThat(promise.future().cause().getMessage()).startsWith("Disallowed view or directive in a subquery");
    }
}
