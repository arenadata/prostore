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
package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.base.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.CoreDtmSettings;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import java.time.ZoneId;

public class TestUtils {
    public static final CalciteConfiguration CALCITE_CONFIGURATION = new CalciteConfiguration();
    public static final CalciteCoreConfiguration CALCITE_CORE_CONFIGURATION = new CalciteCoreConfiguration();
    public static final DefinitionService<SqlNode> DEFINITION_SERVICE =
            new CoreCalciteDefinitionService(CALCITE_CONFIGURATION.configEddlParser(CALCITE_CORE_CONFIGURATION.eddlParserImplFactory()));
    public static final SqlDialect SQL_DIALECT = new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT);
    public static final CoreDtmSettings CORE_DTM_SETTINGS = new CoreDtmSettings(ZoneId.of("UTC"));


    private TestUtils() {
    }

    public static AppConfiguration getCoreConfiguration(String envName) {
        return getCoreAppConfiguration(CORE_DTM_SETTINGS, envName);
    }

    public static AppConfiguration getCoreAppConfiguration(DtmConfig dtmSettings, String envName) {
        return new AppConfiguration(null) {
            @Override
            public String getEnvName() {
                return envName;
            }

            @Override
            public DtmConfig dtmSettings() {
                return dtmSettings;
            }
        };
    }
}
