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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adqm.common.Constants;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service("adqmTruncateHistoryService")
public class AdqmTruncateHistoryService implements TruncateHistoryService {
    private static final String QUERY_PATTER = "INSERT INTO %s.%s_actual (%s, sign)\n" +
            "SELECT %s, -1\n" +
            "FROM %s.%s_actual t FINAL\n" +
            "WHERE sign = 1%s%s";
    private static final String FLUSH_PATTERN = "SYSTEM FLUSH DISTRIBUTED %s.%s_actual";
    private static final String OPTIMIZE_PATTERN = "OPTIMIZE TABLE %s.%s_actual_shard ON CLUSTER %s FINAL";

    private final DatabaseExecutor adqmQueryExecutor;
    private final SqlDialect sqlDialect;
    private final DdlProperties ddlProperties;

    @Autowired
    public AdqmTruncateHistoryService(DatabaseExecutor adqmQueryExecutor,
                                      @Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                                      DdlProperties ddlProperties) {
        this.adqmQueryExecutor = adqmQueryExecutor;
        this.ddlProperties = ddlProperties;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryParams params) {
        String sysCnExpression = params.getSysCn()
                .map(sysCn -> String.format(" AND sys_to < %s", sysCn))
                .orElse("");
        String whereExpression = params.getConditions()
                .map(conditions -> String.format(" AND (%s)", conditions.toSqlString(sqlDialect)))
                .orElse("");
        Entity entity = params.getEntity();
        String dbName = String.format("%s__%s", params.getEnv(), entity.getSchema());
        List<String> orderByColumns = entity.getFields().stream()
                .filter(field -> field.getPrimaryOrder() != null)
                .map(EntityField::getName)
                .collect(Collectors.toList());
        orderByColumns.add(Constants.SYS_FROM_FIELD);
        String orderByColumnsStr = String.join(", ", orderByColumns);
        return adqmQueryExecutor.execute(
                String.format(QUERY_PATTER, dbName, entity.getName(), orderByColumnsStr, orderByColumnsStr, dbName,
                        entity.getName(), sysCnExpression, whereExpression))
                .compose(result -> adqmQueryExecutor.execute(String.format(FLUSH_PATTERN, dbName, entity.getName())))
                .compose(result -> adqmQueryExecutor.execute(
                        String.format(OPTIMIZE_PATTERN, dbName, entity.getName(), ddlProperties.getCluster())))
                .compose(result -> Future.succeededFuture());
    }
}
