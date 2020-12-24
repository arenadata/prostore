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
package io.arenadata.dtm.query.calcite.core.extension.check;

import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlCheckData extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_DATA", SqlKind.CHECK);
    private final String table;
    private final String schema;
    private final Long deltaNum;
    private final Set<String> columns;

    public SqlCheckData(SqlParserPos pos, SqlIdentifier name, SqlLiteral deltaNum, List<SqlNode> columns) {
        super(pos, name);
        final String nameWithSchema = name.toString();
        this.schema = CalciteUtil.parseSchemaName(nameWithSchema);
        this.table = CalciteUtil.parseTableName(nameWithSchema);
        this.deltaNum = deltaNum.longValue(true);
        this.columns = Optional.ofNullable(columns)
                .map(val -> (columns.stream()
                        .map(c -> ((SqlIdentifier) c))
                        .map(SqlIdentifier::getSimple)
                        .collect(Collectors.toSet())))
                .orElse(null);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.DATA;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public Long getDeltaNum() {
        return deltaNum;
    }

    public Set<String> getColumns() {
        return columns;
    }
}
