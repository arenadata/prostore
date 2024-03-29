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

import io.arenadata.dtm.query.execution.plugin.adb.ddl.factory.TruncateQueryFactory;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

public abstract class TruncateQueryBaseFactory implements TruncateQueryFactory {

    private final SqlDialect sqlDialect;

    protected static final String DELETE_RECORDS_PATTERN = "DELETE FROM %s.%s_%s%s";
    protected static final String SYS_CN_CONDITION = "sys_to < %s";

    public TruncateQueryBaseFactory(SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    protected String buildCondition(SqlNode node, String condition) {
        String expression = "";
        if (node != null) {
            expression = String.format(condition, node.toSqlString(sqlDialect));
        }
        return expression;
    }
}
