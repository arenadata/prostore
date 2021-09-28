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
package io.arenadata.dtm.query.calcite.core.util;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

public final class SqlNodeTemplates {

    private SqlNodeTemplates() {
    }

    public static SqlBasicCall as(String origin, String alias) {
        return new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{new SqlIdentifier(origin, SqlParserPos.ZERO),
                new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
    }

    public static SqlBasicCall as(SqlNode origin, String alias) {
        return new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{origin, new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
    }

    public static SqlIdentifier identifier(String... parts) {
        return new SqlIdentifier(Arrays.asList(parts), SqlParserPos.ZERO);
    }

    public static SqlLiteral longLiteral(long value) {
        return SqlLiteral.createExactNumeric(Long.toString(value), SqlParserPos.ZERO);
    }

    public static SqlBasicCall basicCall(SqlOperator sqlOperator, SqlNode left, SqlNode right) {
        return new SqlBasicCall(sqlOperator, new SqlNode[]{left, right}, SqlParserPos.ZERO);
    }

    public static SqlBasicCall basicCall(SqlOperator sqlOperator, SqlNode[] nodes) {
        return new SqlBasicCall(sqlOperator, nodes, SqlParserPos.ZERO);
    }

    public static SqlBasicCall basicCall(SqlOperator sqlOperator, List<SqlNode> nodes) {
        return new SqlBasicCall(sqlOperator, nodes.toArray(new SqlNode[0]), SqlParserPos.ZERO);
    }
}
