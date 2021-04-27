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
package io.arenadata.dtm.query.execution.plugin.adb.ddl.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adb.base.dto.metadata.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.ddl.factory.TruncateHistoryDeleteQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collections;
import java.util.List;

public class TruncateHistoryDeleteQueriesWithoutHistoryFactory implements TruncateHistoryDeleteQueriesFactory {

    private static final String DELETE_RECORDS_PATTERN = "DELETE FROM %s.%s_%s%s";
    private static final String SYS_CN_CONDITION = "sys_to < %s";
    private final SqlDialect sqlDialect;

    public TruncateHistoryDeleteQueriesWithoutHistoryFactory(@Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public List<String> create(TruncateHistoryRequest request) {
        String whereExpression = request.getConditions()
                .map(conditions -> String.format(" WHERE %s", conditions.toSqlString(sqlDialect)))
                .orElse("");
        Entity entity = request.getEntity();
        return Collections.singletonList(String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                AdbTables.ACTUAL_TABLE_POSTFIX, whereExpression));
    }

    @Override
    public String createWithSysCn(TruncateHistoryRequest request) {
        Entity entity = request.getEntity();
        return String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                AdbTables.ACTUAL_TABLE_POSTFIX, String.format(" WHERE %s%s", request.getConditions()
                                .map(conditions -> String.format("%s AND ", conditions.toSqlString(sqlDialect)))
                                .orElse(""),
                        String.format(SYS_CN_CONDITION, request.getSysCn().get())));
    }
}
