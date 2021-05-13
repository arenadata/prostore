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
package io.arenadata.dtm.query.calcite.core.extension.check;

import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlCheckSum extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_SUM", SqlKind.CHECK);
    private final Long deltaNum;
    private final String schema;
    private final String table;
    private final Set<String> columns;

    public SqlCheckSum(SqlParserPos pos, SqlLiteral deltaNum, SqlIdentifier table, List<SqlNode> columns) {
        super(pos, table);
        this.deltaNum = deltaNum.longValue(true);
        this.schema = Optional.ofNullable(table).map(t -> CalciteUtil.parseSchemaName(t.toString())).orElse(null);
        this.table = Optional.ofNullable(table).map(t -> CalciteUtil.parseTableName(t.toString())).orElse(null);
        this.columns = Optional.ofNullable(columns)
                .map(val -> ( columns.stream()
                        .map(c -> (SqlIdentifier) c)
                        .map(SqlIdentifier::getSimple)
                        .collect(Collectors.toCollection(LinkedHashSet::new))))
                .orElse(null);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.SUM;
    }

    public Long getDeltaNum() {
        return deltaNum;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public Set<String> getColumns() {
        return columns;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        String delta = this.deltaNum.toString();
        writer.literal(OPERATOR + "(" + delta + getTableWithColumns() + ")");
    }

    private String getTableWithColumns() {
        if (this.table == null) {
            return "";
        } else {
            final String delimiter = ", ";
            if (this.columns == null) {
                return delimiter + getTableName();
            } else {
                return delimiter + getTableName() + delimiter + getTableColumns();
            }
        }
    }

    private String getTableName() {
        return this.name.toString();
    }

    private String getTableColumns() {
        return "[" + String.join(", ", this.columns) + "]";
    }
}
