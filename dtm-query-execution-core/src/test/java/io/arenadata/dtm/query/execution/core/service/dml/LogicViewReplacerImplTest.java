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
package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.dml.impl.DatamartViewWrapLoaderImpl;
import io.arenadata.dtm.query.execution.core.service.dml.impl.LogicViewReplacerImpl;
import io.arenadata.dtm.query.execution.core.service.dml.impl.SqlSnapshotReplacerImpl;
import io.arenadata.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.service.impl.InformationSchemaServiceImpl;
import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
class LogicViewReplacerImplTest {

    public static final String EXPECTED_WITHOUT_JOIN = "SELECT v.col1 AS c, v.col2 AS r\n" +
        "FROM (SELECT col4, col5\n" +
        "FROM tblx FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'\n" +
        "WHERE tblx.col6 = 0) AS v";

    public static final String EXPECTED_WITH_JOIN = "SELECT v.col1 AS c, v.col2 AS r\n" +
        "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
        "INNER JOIN (SELECT col4, col5\n" +
        "FROM tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
        "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4";

    public static final String EXPECTED_WITH_JOIN_WITHOUT_ALIAS = "SELECT view.col1 AS c, view.col2 AS r\n" +
        "FROM (SELECT col4, col5\n" +
        "FROM tblx\n" +
        "WHERE tblx.col6 = 0) AS view";

    public static final String EXPECTED_WITH_JOIN_AND_WHERE = "SELECT v.col1 AS c, v.col2 AS r\n" +
        "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
        "INNER JOIN (SELECT col4, col5\n" +
        "FROM tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
        "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
        "WHERE EXISTS (SELECT id\n" +
        "FROM (SELECT col4, col5\n" +
        "FROM tblx\n" +
        "WHERE tblx.col6 = 0) AS view)";

    public static final String EXPECTED_WITH_SELECT = "SELECT t.col1 AS c, (SELECT id\n" +
        "FROM (SELECT col4, col5\n" +
        "FROM tblx\n" +
        "WHERE tblx.col6 = 0) AS view\n" +
        "LIMIT 1) AS r\n" +
        "FROM tblt AS t";

    public static final String EXPECTED_WITH_DATAMART = "SELECT v.col1 AS c, v.col2 AS r\n" +
        "FROM (SELECT col4, col5\n" +
        "FROM tblx\n" +
        "WHERE tblx.col6 = 0) AS v";

    public static final String EXPECTED_WITH_VIEW_IN_VIEW = "SELECT v.col1 AS c, v.col2 AS r\n" +
        "FROM (SELECT col4, col5\n" +
        "FROM (SELECT col4, col5\n" +
        "FROM tblc FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'\n" +
        "WHERE tblc.col9 = 0) AS tblz\n" +
        "WHERE tblz.col6 = 0) AS v";

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
        new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));

    private final InformationSchemaService informationSchemaService = mock(InformationSchemaServiceImpl.class);

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
    }

    @Test
    void withoutJoin() throws InterruptedException {
        val testContext = new VertxTestContext();
        val serviceDao = mock(ServiceDbFacade.class);
        Mockito.when(entityDao.getEntity(any(), any()))
            .thenReturn(Future.succeededFuture(Entity.builder()
                .entityType(EntityType.VIEW)
                .name("view")
                .viewQuery("SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0")
                .build()));

        val loader = new DatamartViewWrapLoaderImpl(serviceDao, informationSchemaService);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
            "FROM view FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITHOUT_JOIN, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withDatamart() throws InterruptedException {
        val testContext = new VertxTestContext();

        Mockito.when(entityDao.getEntity(any(), any()))
            .thenReturn(Future.succeededFuture(Entity.builder()
                .entityType(EntityType.VIEW)
                .name("view")
                .viewQuery("SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0")
                .build()));

        val loader = new DatamartViewWrapLoaderImpl(serviceDbFacade, informationSchemaService);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
            "FROM test.view v";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_DATAMART, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withoutJoin_withoutAlias() throws InterruptedException {
        val testContext = new VertxTestContext();

        Mockito.when(entityDao.getEntity(any(), any()))
            .thenReturn(Future.succeededFuture(Entity.builder()
                .entityType(EntityType.VIEW)
                .name("view")
                .viewQuery("SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0")
                .build()));

        val loader = new DatamartViewWrapLoaderImpl(serviceDbFacade, informationSchemaService);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT view.Col1 as c, view.Col2 r\n" +
            "FROM view";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_JOIN_WITHOUT_ALIAS, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withJoin() throws InterruptedException {
        val testContext = new VertxTestContext();

        Mockito.when(entityDao.getEntity(any(), any()))
            .thenReturn(Future.succeededFuture(Entity.builder()
                .entityType(EntityType.VIEW)
                .name("view")
                .viewQuery("SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0")
                .build()));

        val loader = new DatamartViewWrapLoaderImpl(serviceDbFacade, informationSchemaService);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
            "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' t\n" +
            "JOIN view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' v\n" +
            "ON t.Col3 = v.Col4";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_JOIN, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withJoinAndWhere() throws InterruptedException {
        val testContext = new VertxTestContext();

        Mockito.when(entityDao.getEntity(any(), any()))
            .thenReturn(Future.succeededFuture(Entity.builder()
                .entityType(EntityType.VIEW)
                .name("view")
                .viewQuery("SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0")
                .build()));

        val loader = new DatamartViewWrapLoaderImpl(serviceDbFacade, informationSchemaService);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
            "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' t\n" +
            "JOIN view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' v\n" +
            "ON t.Col3 = v.Col4 \n" +
            "WHERE exists (select id from view)";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_JOIN_AND_WHERE, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withJoinAndSelect() throws InterruptedException {
        val testContext = new VertxTestContext();
        Mockito.when(entityDao.getEntity(any(), any()))
            .thenReturn(Future.succeededFuture(Entity.builder()
                .entityType(EntityType.VIEW)
                .name("view")
                .viewQuery("SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0")
                .build()));
        val loader = new DatamartViewWrapLoaderImpl(serviceDbFacade, informationSchemaService);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT t.Col1 as c, (select id from view limit 1) r\n" +
            "FROM tblt t";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_SELECT, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void viewInView() throws InterruptedException {
        val testContext = new VertxTestContext();
        Mockito.when(entityDao.getEntity(any(), any()))
            .thenReturn(
                Future.succeededFuture(Entity.builder()
                    .entityType(EntityType.VIEW)
                    .name("view")
                    .viewQuery("SELECT Col4, Col5 \n" +
                        "FROM tblZ \n" +
                        "WHERE tblZ.Col6 = 0")
                    .build()),
                Future.succeededFuture(Entity.builder()
                    .entityType(EntityType.VIEW)
                    .name("tblZ")
                    .viewQuery("SELECT Col4, Col5 \n" +
                        "FROM tblC \n" +
                        "WHERE tblC.Col9 = 0")
                    .build())
            );
        val loader = new DatamartViewWrapLoaderImpl(serviceDbFacade, informationSchemaService);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
            "FROM view FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_VIEW_IN_VIEW, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }
}
