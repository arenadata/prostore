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

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.SystemMetadata;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.service.dml.impl.UseSchemaDmlExecutor;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.service.metrics.impl.MetricsServiceImpl;
import io.arenadata.dtm.query.execution.core.utils.ParseQueryUtils;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DmlRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UseSchemaDmlExecutorTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final ParseQueryUtils parseQueryUtils = mock(ParseQueryUtils.class);
    private final MetricsService<RequestMetrics> metricsService = mock(MetricsServiceImpl.class);
    private UseSchemaDmlExecutor useSchemaDdlExecutor;
    private DmlRequestContext context;
    private String schema = "shares";

    @BeforeEach
    void setUp() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(parseQueryUtils.getDatamartName(anyList())).thenReturn(schema);
        useSchemaDdlExecutor = new UseSchemaDmlExecutor(serviceDbFacade, parseQueryUtils, metricsService);
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("USE shares");
        SqlNode query = planner.parse(queryRequest.getSql());
        context = new DmlRequestContext(new RequestMetrics(), new DmlRequest(queryRequest), query);
        context.getRequest().setQueryRequest(queryRequest);
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        QueryResult result = new QueryResult();
        result.setMetadata(Collections.singletonList(
                ColumnMetadata.builder()
                        .name("schema")
                        .systemMetadata(SystemMetadata.SCHEMA)
                        .type(ColumnType.VARCHAR).build()));

        result.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        result.setResult(QueryResultUtils.createResultWithSingleRow(Collections.singletonList("schema"),
                Collections.singletonList(schema)));

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        when(metricsService.sendMetrics(any(), any(), any(), any()))
                .thenReturn(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(result);
                    } else {
                        promise.fail(ar.cause());
                    }
                });

        useSchemaDdlExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(result, promise.future().result());
        assertEquals(schema, ((QueryResult) promise.future().result()).getResult().get(0).get("schema"));
    }

    @Test
    void executeDatamartIsNotExists() {
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(false));

        when(metricsService.sendMetrics(any(), any(), any(), any()))
                .thenReturn(ar -> {
                    promise.fail(ar.cause());
                });

        useSchemaDdlExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeIncorrectQuery() {
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        when(metricsService.sendMetrics(any(), any(), any(), any())).thenReturn(ar -> {
            if (ar.succeeded()) {
                promise.complete(null);
            } else {
                promise.fail(ar.cause());
            }
        });

        useSchemaDdlExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
}
