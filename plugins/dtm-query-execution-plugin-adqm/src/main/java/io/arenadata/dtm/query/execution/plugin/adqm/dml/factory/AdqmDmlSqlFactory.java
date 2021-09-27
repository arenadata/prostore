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
package io.arenadata.dtm.query.execution.plugin.adqm.dml.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class AdqmDmlSqlFactory {
    private static final String FLUSH_SQL_TEMPLATE = "SYSTEM FLUSH DISTRIBUTED %s__%s.%s_actual";
    private static final String OPTIMIZE_SQL_TEMPLATE = "OPTIMIZE TABLE %s__%s.%s_actual_shard ON CLUSTER %s FINAL";

    private final DdlProperties ddlProperties;
    private final SqlDialect sqlDialect;

    public AdqmDmlSqlFactory(DdlProperties ddlProperties,
                             @Qualifier("adqmSqlDialect") SqlDialect sqlDialect) {
        this.ddlProperties = ddlProperties;
        this.sqlDialect = sqlDialect;
    }

    public String getFlushSql(String env, String datamart, String entityName) {
        return String.format(FLUSH_SQL_TEMPLATE, env, datamart, entityName);
    }

    public String getOptimizeSql(String env, String datamart, String entityName) {
        return String.format(OPTIMIZE_SQL_TEMPLATE, env, datamart, entityName, ddlProperties.getCluster());
    }

    public String getSqlFromNodes(SqlNode... sqlNodes) {
        return Stream.of(sqlNodes)
                .map(sqlNode -> sqlNode.toSqlString(sqlDialect).getSql().replaceAll("\r\n|\r|\n", " "))
                .collect(Collectors.joining(" "));
    }

}
