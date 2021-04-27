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
package io.arenadata.dtm.query.calcite.core.delta.service;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.service.DeltaService;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.DeltaInformationExtractor;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.DeltaInformationExtractorImpl;
import io.arenadata.dtm.query.calcite.core.service.impl.DeltaQueryPreprocessorImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DeltaQueryPreprocessorImplTest {

    private final DefinitionService<SqlNode> definitionService = mock(CalciteDefinitionService.class);
    private final DeltaService deltaService = mock(DeltaService.class);
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DeltaInformationExtractor deltaInformationExtractor = new DeltaInformationExtractorImpl(new DtmConfig() {
        @Override
        public ZoneId getTimeZone() {
            return ZoneId.of("UTC");
        }
    });
    private SqlParser.Config parserConfig;
    private DeltaQueryPreprocessor deltaQueryPreprocessor;
    private Planner planner;
    //FIXME fix after refactoring
    @BeforeEach
    void setUp() {
        parserConfig = SqlParser.configBuilder()
            .setParserFactory(calciteCoreConfiguration.eddlParserImplFactory())
            .setConformance(SqlConformanceEnum.DEFAULT)
            .setLex(Lex.MYSQL)
            .setCaseSensitive(false)
            .setUnquotedCasing(Casing.TO_LOWER)
            .setQuotedCasing(Casing.TO_LOWER)
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .build();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        deltaQueryPreprocessor = new DeltaQueryPreprocessorImpl(deltaService, deltaInformationExtractor);
    }

    @Test
    void processWithDeltaNums() throws SqlParseException {
        Promise promise = Promise.promise();
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
            "FROM test.tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
            "INNER JOIN (SELECT col4, col5\n" +
            "FROM test2.tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
            "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
            "WHERE EXISTS (SELECT id\n" +
            "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        final SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInfoList = Arrays.asList(
            new DeltaInformation("t3", "2018-07-29 23:59:59", false,
                DeltaType.NUM, 1L, null, "test_datamart", "tblc", pos),
            new DeltaInformation("", "2019-12-23 15:15:14", false,
                DeltaType.NUM, 3L, null, "test", "tbl", pos),
            new DeltaInformation("", "2019-12-23 15:15:14", false,
                DeltaType.NUM, 3L, null, "test2", "tblx", pos),
            new DeltaInformation("", "2019-12-23 15:15:14", false,
                DeltaType.NUM, 3L, null, "test_datamart", "tblz", pos)
        );

        QueryRequest request = new QueryRequest();
        request.setDatamartMnemonic("test_datamart");
        //request.setDeltaInformations(deltaInfoList);
        request.setRequestId(UUID.randomUUID());
        //request.setEnvName("local");
        //request.setSourceType(SourceType.ADB);
        request.setSql(sql);
        when(definitionService.processingQuery(any())).thenReturn(sqlNode);

        Mockito.when(deltaService.getCnToByDeltaDatetime(any(), any())).thenReturn(Future.succeededFuture(1L))
            .thenReturn(Future.succeededFuture(2L))
            .thenReturn(Future.succeededFuture(3L))
            .thenReturn(Future.succeededFuture(4L));

        deltaQueryPreprocessor.process(sqlNode)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            });

        assertNotNull(promise.future().result());
        //assertEquals(4, ((QueryRequest) promise.future().result()).getDeltaInformations().size());
    }

    @Test
    void processWithDeltaNumIntervals() throws SqlParseException {
        Promise promise = Promise.promise();
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF DELTA_NUM 1 t3 WHERE tblx.col6 = 0 ) AS r\n" +
            "FROM test.tbl FOR SYSTEM_TIME AS OF DELTA_NUM 2 AS t\n" +
            "INNER JOIN (SELECT col4, col5\n" +
            "FROM test2.tblx FOR SYSTEM_TIME STARTED IN (3,4)\n" +
            "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
            "WHERE EXISTS (SELECT id\n" +
            "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME FINISHED IN (3,4) WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        List<Long> deltas = new ArrayList<>();
        final SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInfoList = Arrays.asList(
            new DeltaInformation("t3", "2018-07-29 23:59:59", false,
                DeltaType.NUM, 1L, null, "test_datamart", "tblc", pos),
            new DeltaInformation("", "2019-12-23 15:15:14", false,
                DeltaType.NUM, 2L, null, "test", "tbl", pos),
            new DeltaInformation("", "2019-12-23 15:15:14", false,
                DeltaType.STARTED_IN, null, new SelectOnInterval(3L, 4L), "test2", "tblx", pos),
            new DeltaInformation("", "2019-12-23 15:15:14", false,
                DeltaType.FINISHED_IN, null, new SelectOnInterval(3L, 4L), "test_datamart", "tblz", pos)
        );

        QueryRequest request = new QueryRequest();
        request.setDatamartMnemonic("test_datamart");
        //request.setDeltaInformations(deltaInfoList);
        request.setRequestId(UUID.randomUUID());
        //request.setEnvName("local");
        //request.setSourceType(SourceType.ADB);
        request.setSql(sql);
        when(definitionService.processingQuery(any())).thenReturn(sqlNode);

        Mockito.when(deltaService.getCnToByDeltaNum(any(), eq(1L))).thenReturn(Future.succeededFuture(-1L));
        Mockito.when(deltaService.getCnToByDeltaNum(any(), eq(2L))).thenReturn(Future.succeededFuture(-1L));

        RuntimeException ex = new RuntimeException("delta range error");
        Mockito.when(deltaService.getCnFromCnToByDeltaNums(any(), eq(3L), eq(4L))).thenReturn(Future.failedFuture(ex));

        deltaQueryPreprocessor.process(sqlNode)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            });

        assertNotNull(promise.future().cause());
    }

    @Test
    void processWithDeltaNumAndIntervals() throws SqlParseException {
        Promise promise = Promise.promise();
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
            "FROM test.tbl FOR SYSTEM_TIME AS OF DELTA_NUM 2 AS t\n" +
            "INNER JOIN (SELECT col4, col5\n" +
            "FROM test2.tblx FOR SYSTEM_TIME STARTED IN (3,4)\n" +
            "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
            "WHERE EXISTS (SELECT id\n" +
            "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME FINISHED IN (3,4) WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        List<Long> deltas = Arrays.asList(1L, 2L);
        final SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInfoList = Arrays.asList(
            new DeltaInformation("t3", "2018-07-29 23:59:59", false,
                DeltaType.DATETIME, null, null, "test_datamart", "tblc", pos),
            new DeltaInformation("", "2019-12-23 15:15:14", false
                , DeltaType.NUM, 2L, null, "test", "tbl", pos),
            new DeltaInformation("", "2019-12-23 15:15:14", false,
                DeltaType.STARTED_IN, 0L, new SelectOnInterval(3L, 4L), "test2", "tblx", pos),
            new DeltaInformation("", "2019-12-23 15:15:14", false,
                DeltaType.FINISHED_IN, 0L, new SelectOnInterval(3L, 4L), "test_datamart", "tblz", pos)
        );

        QueryRequest request = new QueryRequest();
        request.setDatamartMnemonic("test_datamart");
        //request.setDeltaInformations(deltaInfoList);
        request.setRequestId(UUID.randomUUID());
        //request.setEnvName("local");
        //request.setSourceType(SourceType.ADB);
        request.setSql(sql);
        when(definitionService.processingQuery(any())).thenReturn(sqlNode);

        Mockito.when(deltaService.getCnToByDeltaDatetime(any(), any())).thenReturn(Future.succeededFuture(1L));
        Mockito.when(deltaService.getCnToByDeltaNum(any(), eq(2L))).thenReturn(Future.succeededFuture(2L));

        SelectOnInterval interval = new SelectOnInterval(3L, 4L);
        Mockito.when(deltaService.getCnFromCnToByDeltaNums(any(), eq(3L), eq(4L))).thenReturn(Future.succeededFuture(interval));

        deltaQueryPreprocessor.process(sqlNode)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            });

        assertNotNull(promise.future().result());
        //assertEquals(4, ((QueryRequest) promise.future().result()).getDeltaInformations().size());
        //assertEquals(Collections.singletonList(1L), ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
        //    .filter(d -> d.getType().equals(DeltaType.DATETIME))
        //    .map(DeltaInformation::getSelectOnNum).collect(Collectors.toList()));
        //assertEquals(Collections.singletonList(2L), ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
        //    .filter(d -> d.getType().equals(DeltaType.NUM))
        //    .map(DeltaInformation::getSelectOnNum).collect(Collectors.toList()));
        //assertEquals(Collections.singletonList(interval),
        //    ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
        //        .filter(d -> d.getType().equals(DeltaType.STARTED_IN))
        //        .map(DeltaInformation::getSelectOnInterval).collect(Collectors.toList()));
        //assertEquals(Collections.singletonList(interval),
        //    ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
        //        .filter(d -> d.getType().equals(DeltaType.FINISHED_IN))
        //        .map(DeltaInformation::getSelectOnInterval).collect(Collectors.toList()));
    }
}
