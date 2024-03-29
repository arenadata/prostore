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
package io.arenadata.dtm.query.execution.core.ddl.view;

import io.arenadata.dtm.cache.service.CacheService;
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
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.*;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.view.CreateViewExecutor;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
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

import java.util.*;

import static io.arenadata.dtm.query.execution.core.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class CreateViewExecutorTest {


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
    private final OkDelta deltaOk = OkDelta.builder()
            .deltaNum(1)
            .build();

    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;
    @Mock
    private EntityDao entityDao;
    @Mock
    private DatamartDao datamartDao;
    @Mock
    private ChangelogDao changelogDao;
    @Mock
    private DeltaServiceDao deltaServiceDao;
    @Mock
    private LogicalSchemaProvider logicalSchemaProvider;
    @Mock
    private ColumnMetadataService columnMetadataService;
    @Mock
    private MetadataExecutor<DdlRequestContext> metadataExecutor;
    @Mock
    private CacheService<EntityKey, Entity> entityCacheService;
    @Mock
    private QueryParserService parserService;
    @Captor
    private ArgumentCaptor<Entity> entityArgumentCaptor;
    @Captor
    private ArgumentCaptor<String> changeQueryCaptor;

    private Planner planner;
    private CreateViewExecutor createViewExecutor;
    private String sqlNodeName;
    private String schema;
    private List<Datamart> logicSchema;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        lenient().when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        lenient().when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        lenient().when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        lenient().when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        lenient().when(serviceDbDao.getChangelogDao()).thenReturn(changelogDao);

        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        createViewExecutor = new CreateViewExecutor(metadataExecutor,
                serviceDbFacade,
                SQL_DIALECT,
                entityCacheService,
                logicalSchemaProvider,
                columnMetadataService,
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
        lenient().when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        lenient().when(logicalSchemaProvider.getSchemaFromQuery(any(), any()))
                .thenReturn(Future.succeededFuture(logicSchema));
    }

    @Test
    void executeSuccess(VertxTestContext testContext) throws SqlParseException {
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

        when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(entityArgumentCaptor.capture(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.succeededFuture());


        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val value = entityArgumentCaptor.getValue();
                    assertEquals("SELECT id FROM shares.test_table", value.getViewQuery());
                    assertEquals("CREATE VIEW shares.test_view AS\n" +
                            "SELECT id\n" +
                            "FROM shares.test_table", changeQueryCaptor.getValue());
                }).completeNow());
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

        when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(entityArgumentCaptor.capture(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("")));

        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.UPDATE)))
                .thenReturn(Future.succeededFuture());

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals("CREATE OR REPLACE VIEW shares.test_view AS\n" +
                "SELECT id\n" +
                "FROM shares.test_table", changeQueryCaptor.getValue());
    }

    @Test
    void executeCreateEntityError(VertxTestContext testContext) throws SqlParseException {
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

        when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.failedFuture(new DtmException("create entity error")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals("create entity error", ar.cause().getMessage());
                }).completeNow());
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
    void executeContainsCollateError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s WHERE varchar_col = 'test' COLLATE 'unicode_ci'",
                schema, entityList.get(0).getName(), schema, entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeEntityAlreadyExistError(VertxTestContext testContext) throws SqlParseException {
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

        when(deltaServiceDao.getDeltaOk(schema))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), changeQueryCaptor.capture(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("entity already exist")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof EntityAlreadyExistsException);
                    assertEquals("CREATE VIEW shares.test_view AS\n" +
                            "SELECT id\n" +
                            "FROM shares.test_table", changeQueryCaptor.getValue());
                }).completeNow());
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

        when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals(String.format("Entity %s does not exist", entityList.get(1).getName()), promise.future().cause().getMessage());
    }

    @Test
    void executeInnerJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "INNER");
    }

    @Test
    void executeFullJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "FULL");
    }

    @Test
    void executeLeftJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "LEFT");
    }

    @Test
    void executeRightJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "RIGHT");
    }

    @Test
    void executeCrossJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "CROSS");
    }

    @Test
    void executeMultipleJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s " +
                        "JOIN %s.%s ON %s.id = %s.id " +
                        "JOIN %s.%s ON %s.id = %s.id",
                schema, entityList.get(1).getName(), schema, entityList.get(2).getName(),
                schema, entityList.get(3).getName(), entityList.get(2).getName(), entityList.get(3).getName(),
                schema, entityList.get(0).getName(), entityList.get(2).getName(), entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.getEntity(schema, entityList.get(3).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(3)));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertThat(ar.cause().getMessage()).startsWith("Disallowed view or directive in a subquery");
                }).completeNow());
    }

    @Test
    void executeWithTimestampSuccess(VertxTestContext testContext) throws SqlParseException {
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

        when(entityDao.getEntity(schema, entityList.get(6).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(6)));

        when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.succeededFuture());

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertEquals("CREATE VIEW shares.test_view AS\n" +
                            "SELECT id, timestamp_col\n" +
                            "FROM shares.entity\n" +
                            "WHERE timestamp_col = '2020-12-01 00:00:00'", changeQueryCaptor.getValue());
                }).completeNow());
    }

    @Test
    void executeWrongTimestampFormatError(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s WHERE timestamp_col = '123456'",
                schema, entityList.get(0).getName(), schema, entityList.get(6).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(entityDao.getEntity(schema, entityList.get(6).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(6)));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof ValidationDtmException);
                }).completeNow());
    }

    private void testJoinWithWrongEntityType(VertxTestContext testContext, String joinType) throws SqlParseException {
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

        lenient().when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        lenient().when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        lenient().when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        lenient().when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        lenient().when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        lenient().when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertThat(ar.cause().getMessage()).startsWith("Disallowed view or directive in a subquery");
                }).completeNow());
    }
}
