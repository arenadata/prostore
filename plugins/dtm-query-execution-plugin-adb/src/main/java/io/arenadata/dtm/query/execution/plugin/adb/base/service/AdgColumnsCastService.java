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
package io.arenadata.dtm.query.execution.plugin.adb.base.service;

import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicatePart;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicates;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.vertx.core.Future;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Service
public class AdgColumnsCastService {

    private static final SqlPredicates COLUMN_SELECT = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eqFromStart(SqlKind.SELECT))
            .anyOf(SqlPredicatePart.eq(SqlKind.OTHER))
            .build();

    private final SqlDialect sqlDialect;

    public AdgColumnsCastService(@Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    public Future<SqlNode> replaceTimeBasedColumns(QueryParserResponse parserResponse) {
        return replaceTimeBasedColumns(parserResponse.getSqlNode(), parserResponse.getRelNode().rel);
    }

    public Future<SqlNode> replaceTimeBasedColumns(SqlNode sqlNode, RelNode relNode) {
        return Future.future(promise -> {
            SqlSelectTree sqlNodeTree = new SqlSelectTree(sqlNode);
            List<SqlTreeNode> columnsNode = sqlNodeTree.findNodes(COLUMN_SELECT, true);
            if (columnsNode.size() != 1) {
                throw new DtmException(format("Expected one node contain columns: %s", sqlNode.toSqlString(sqlDialect).toString()));
            }

            List<SqlTreeNode> columnsNodes = sqlNodeTree.findNodesByParent(columnsNode.get(0));
            List<SqlTypeName> columnsTypes = relNode
                    .getRowType()
                    .getFieldList()
                    .stream()
                    .map(RelDataTypeField::getType)
                    .map(RelDataType::getSqlTypeName)
                    .collect(Collectors.toList());

            for (int i = 0; i < columnsNodes.size(); i++) {
                SqlTypeName columnType = columnsTypes.get(i);
                if (isNotTimeType(columnType)) {
                    continue;
                }

                SqlTreeNode columnNode = columnsNodes.get(i);

                if (columnNode.getNode().getKind() == SqlKind.AS) {
                    columnNode = sqlNodeTree.findNodesByParent(columnNode).get(0);
                }

                columnNode.getSqlNodeSetter().accept(surroundWith(columnType, columnNode.getNode()));
            }

            promise.complete(sqlNode);
        });
    }

    private SqlNode surroundWith(SqlTypeName columnType, SqlNode nodeToSurround) {
        SqlParserPos parserPosition = nodeToSurround.getParserPosition();
        SqlIntervalQualifier epoch = new SqlIntervalQualifier(TimeUnit.EPOCH, TimeUnit.EPOCH, parserPosition);
        SqlNode extract = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{epoch, nodeToSurround}, parserPosition);
        SqlDataTypeSpec bigintType = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, parserPosition), parserPosition);
        switch (columnType) {
            case DATE: {
                SqlNode divide = new SqlBasicCall(SqlStdOperatorTable.DIVIDE, new SqlNode[]{extract, SqlLiteral.createExactNumeric("86400", parserPosition)}, parserPosition);
                return new SqlBasicCall(SqlStdOperatorTable.CAST, new SqlNode[]{divide, bigintType}, parserPosition);
            }
            case TIME:
            case TIMESTAMP: {
                SqlNode multiply = new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, new SqlNode[]{extract, SqlLiteral.createExactNumeric("1000000", parserPosition)}, parserPosition);
                return new SqlBasicCall(SqlStdOperatorTable.CAST, new SqlNode[]{multiply, bigintType}, parserPosition);
            }
            default:
                throw new IllegalArgumentException("Invalid type to surround");
        }
    }

    private boolean isNotTimeType(SqlTypeName columnType) {
        switch (columnType) {
            case TIMESTAMP:
            case TIME:
            case DATE:
                return false;
            default:
                return true;
        }
    }
}
