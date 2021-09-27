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
package io.arenadata.dtm.query.execution.plugin.adqm.ddl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.ddl.service.AdqmTruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.adqm.ddl.factory.AdqmTruncateHistoryQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class AdqmTruncateHistoryServiceTest {
    private static final String ENV = "env";
    private static final String CLUSTER = "cluster";
    private static final String EXPECTED_PATTERN = "INSERT INTO %s__%s.%s_actual (%s, sign)\n" +
            "SELECT %s, -1\n" +
            "FROM %s__%s.%s_actual t FINAL\n" +
            "WHERE sign = 1%s%s";
    private final CalciteConfiguration calciteConfiguration = TestUtils.CALCITE_CONFIGURATION;
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
            .configDdlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    private final DatabaseExecutor adqmQueryExecutor = mock(AdqmQueryExecutor.class);
    private AdqmTruncateHistoryQueriesFactory queriesFactory;
    private TruncateHistoryService adqmTruncateHistoryService;
    private Entity entity;
    private String orderByColumns;

    @BeforeEach
    void setUp() {
        entity = AdqmCreateTableQueriesFactoryTest.getEntity();
        orderByColumns = entity.getFields().stream()
                .filter(entityField -> entityField.getPrimaryOrder() != null)
                .map(EntityField::getName)
                .collect(Collectors.joining(", "));
        orderByColumns += String.format(", %s", Constants.SYS_FROM_FIELD);
        DdlProperties ddlProperties = new DdlProperties();
        ddlProperties.setCluster(CLUSTER);
        queriesFactory = new AdqmTruncateHistoryQueriesFactory(calciteConfiguration.adqmSqlDialect(), ddlProperties);
        adqmTruncateHistoryService = new AdqmTruncateHistoryService(adqmQueryExecutor,
                queriesFactory);
        when(adqmQueryExecutor.execute(anyString())).thenReturn(Future.succeededFuture());
    }

    @Test
    void test() {
        String expected = String.format(EXPECTED_PATTERN, ENV, entity.getSchema(), entity.getName(),
                orderByColumns, orderByColumns, ENV, entity.getSchema(), entity.getName(), "", "");
        test(null, null, expected);
    }

    @Test
    void testWithConditions() {
        String conditions = "id > 2";
        String expected = String.format(EXPECTED_PATTERN, ENV, entity.getSchema(), entity.getName(),
                orderByColumns, orderByColumns, ENV, entity.getSchema(), entity.getName(), "",
                String.format(" AND (%s)", conditions));
        test(null, conditions, expected);
    }

    @Test
    void testWithSysCn() {
        Long sysCn = 1L;
        String expected = String.format(EXPECTED_PATTERN, ENV, entity.getSchema(), entity.getName(),
                orderByColumns, orderByColumns, ENV, entity.getSchema(), entity.getName(),
                String.format(" AND sys_to < %s", sysCn), "");
        test(sysCn, null, expected);
    }


    @Test
    void testWithConditionsAndSysCn() {
        String conditions = "id > 2";
        Long sysCn = 1L;
        String expected = String.format(EXPECTED_PATTERN, ENV, entity.getSchema(), entity.getName(),
                orderByColumns, orderByColumns, ENV, entity.getSchema(), entity.getName(),
                String.format(" AND sys_to < %s", sysCn), String.format(" AND (%s)", conditions));
        test(sysCn, conditions, expected);
    }

    private void test(Long sysCn, String conditions, String expected) {
        SqlNode sqlNode = Optional.ofNullable(conditions)
                .map(val -> {
                    try {
                        return ((SqlSelect) planner.parse(String.format("SELECT * from t WHERE %s", conditions)))
                                .getWhere();
                    } catch (SqlParseException e) {
                        throw new DataSourceException("Error", e);
                    }
                })
                .orElse(null);
        TruncateHistoryRequest request = TruncateHistoryRequest.builder()
                .sysCn(sysCn)
                .entity(entity)
                .envName(ENV)
                .conditions(sqlNode)
                .build();
        adqmTruncateHistoryService.truncateHistory(request);
        verify(adqmQueryExecutor, times(1)).execute(expected);
        verify(adqmQueryExecutor, times(1)).execute(
                String.format("SYSTEM FLUSH DISTRIBUTED %s__%s.%s_actual", ENV, entity.getSchema(), entity.getName()));
        verify(adqmQueryExecutor, times(1)).execute(
                String.format("OPTIMIZE TABLE %s__%s.%s_actual_shard ON CLUSTER %s FINAL", ENV, entity.getSchema(),
                        entity.getName(), CLUSTER));
    }
}
