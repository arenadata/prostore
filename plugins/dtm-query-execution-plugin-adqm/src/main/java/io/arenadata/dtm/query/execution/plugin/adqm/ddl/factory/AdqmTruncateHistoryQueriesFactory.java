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
package io.arenadata.dtm.query.execution.plugin.adqm.ddl.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants;
import io.arenadata.dtm.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class AdqmTruncateHistoryQueriesFactory {

    private static final String QUERY_PATTERN = "INSERT INTO %s.%s_actual (%s, sign)\n" +
            "SELECT %s, -1\n" +
            "FROM %s.%s_actual t FINAL\n" +
            "WHERE sign = 1%s%s";
    private static final String FLUSH_PATTERN = "SYSTEM FLUSH DISTRIBUTED %s.%s_actual";
    private static final String OPTIMIZE_PATTERN = "OPTIMIZE TABLE %s.%s_actual_shard ON CLUSTER %s FINAL";

    private final SqlDialect sqlDialect;
    private final DdlProperties ddlProperties;

    @Autowired
    public AdqmTruncateHistoryQueriesFactory(@Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                                             DdlProperties ddlProperties) {
        this.sqlDialect = sqlDialect;
        this.ddlProperties = ddlProperties;
    }

    public String insertIntoActualQuery(TruncateHistoryRequest request) {
        String sysCnExpression = request.getSysCn() != null ?
                String.format(" AND sys_to < %s", request.getSysCn()) : "";
        String whereExpression = request.getConditions() != null ?
                String.format(" AND (%s)", request.getConditions().toSqlString(sqlDialect)) : "";

        Entity entity = request.getEntity();
        String dbName = Constants.getDbName(request.getEnvName(), entity.getSchema());
        List<String> orderByColumns = entity.getFields().stream()
                .filter(field -> field.getPrimaryOrder() != null)
                .map(EntityField::getName)
                .collect(Collectors.toList());
        orderByColumns.add(Constants.SYS_FROM_FIELD);
        String orderByColumnsStr = String.join(", ", orderByColumns);
        return String.format(QUERY_PATTERN, dbName, entity.getName(), orderByColumnsStr, orderByColumnsStr, dbName,
                        entity.getName(), sysCnExpression, whereExpression);
    }

    public String flushQuery(TruncateHistoryRequest request) {
        Entity entity = request.getEntity();
        return String.format(FLUSH_PATTERN, Constants.getDbName(request.getEnvName(), entity.getSchema()),
                entity.getName());
    }

    public String optimizeQuery(TruncateHistoryRequest request) {
        Entity entity = request.getEntity();
        return String.format(OPTIMIZE_PATTERN, Constants.getDbName(request.getEnvName(), entity.getSchema()),
                entity.getName(), ddlProperties.getCluster());
    }

}
