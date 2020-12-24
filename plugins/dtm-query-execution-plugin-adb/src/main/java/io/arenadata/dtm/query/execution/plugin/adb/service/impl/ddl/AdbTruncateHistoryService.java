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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service("adbTruncateHistoryService")
public class AdbTruncateHistoryService implements TruncateHistoryService {
    private static final String DELETE_RECORDS_PATTERN = "DELETE FROM %s.%s_%s%s";
    private static final String SYS_CN_CONDITION = "sys_to < %s";
    private final DatabaseExecutor adbQueryExecutor;
    private final SqlDialect sqlDialect;

    @Autowired
    public AdbTruncateHistoryService(DatabaseExecutor adbQueryExecutor,
                                     @Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryParams params) {
        return params.getSysCn().isPresent() ? executeWithSysCn(params) : execute(params);
    }

    private Future<Void> execute(TruncateHistoryParams params) {
        String whereExpression = params.getConditions()
                .map(conditions -> String.format(" WHERE %s", conditions.toSqlString(sqlDialect)))
                .orElse("");
        Entity entity = params.getEntity();
        List<String> queries = Arrays.asList(String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                AdbTables.ACTUAL_TABLE_POSTFIX, whereExpression),
                String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                        AdbTables.HISTORY_TABLE_POSTFIX, whereExpression));
        return Future.future(promise -> adbQueryExecutor.executeInTransaction(queries.stream()
                        .map(PreparedStatementRequest::onlySql)
                        .collect(Collectors.toList()),
                promise));
    }

    private Future<Void> executeWithSysCn(TruncateHistoryParams params) {
        Entity entity = params.getEntity();
        String query = String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                AdbTables.HISTORY_TABLE_POSTFIX, String.format(" WHERE %s%s", params.getConditions()
                                .map(conditions -> String.format("%s AND ", conditions.toSqlString(sqlDialect)))
                                .orElse(""),
                        String.format(SYS_CN_CONDITION, params.getSysCn().get())));
        return adbQueryExecutor.execute(query)
                .compose(result -> Future.succeededFuture());
    }
}
