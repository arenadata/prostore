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
package io.arenadata.dtm.query.calcite.core.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicatePart;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicates;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractQueryTemplateExtractor implements QueryTemplateExtractor {
    public static final SqlPredicates DYNAMIC_PARAM_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eq(SqlKind.DYNAMIC_PARAM))
            .build();
    private static final SqlDynamicParam DYNAMIC_PARAM = new SqlDynamicParam(0, SqlParserPos.QUOTED_ZERO);
    private static final SqlPredicates SET_DYNAMIC_PARAM = SqlPredicates.builder()
            .anyOf(
                    SqlPredicatePart.eqWithNum(SqlKind.LIKE),
                    SqlPredicatePart.eqWithNum(SqlKind.EQUALS),
                    SqlPredicatePart.eqWithNum(SqlKind.NOT_EQUALS),
                    SqlPredicatePart.eqWithNum(SqlKind.PERIOD_EQUALS),
                    SqlPredicatePart.eqWithNum(SqlKind.LESS_THAN_OR_EQUAL),
                    SqlPredicatePart.eqWithNum(SqlKind.LESS_THAN),
                    SqlPredicatePart.eqWithNum(SqlKind.GREATER_THAN_OR_EQUAL),
                    SqlPredicatePart.eqWithNum(SqlKind.GREATER_THAN),
                    SqlPredicatePart.eqWithNum(SqlKind.BETWEEN),
                    SqlPredicatePart.eqWithNum(SqlKind.IN),
                    SqlPredicatePart.eqWithNum(SqlKind.NOT_IN),
                    SqlPredicatePart.eqWithNum(SqlKind.DYNAMIC_PARAM)
            )
            .build();
    private static final String COLLATE_OPERATOR = "COLLATE";
    private final DefinitionService<SqlNode> definitionService;
    private final SqlDialect sqlDialect;

    protected AbstractQueryTemplateExtractor(DefinitionService<SqlNode> definitionService, SqlDialect sqlDialect) {
        this.definitionService = definitionService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public QueryTemplateResult extract(String sql) {
        return extract(definitionService.processingQuery(sql));
    }

    @Override
    public QueryTemplateResult extract(String sql, List<String> excludeColumns) {
        return extract(definitionService.processingQuery(sql), excludeColumns);
    }

    @Override
    public SqlNode enrichTemplate(EnrichmentTemplateRequest request) {
        //TODO perhaps it will be better to move method of enriching query template to separate interface
        // and implement it in different plugin classes
        SqlSelectTree selectTree = new SqlSelectTree(SqlNodeUtil.copy(request.getTemplateNode()));
        List<SqlTreeNode> dynamicNodes = selectTree.findNodes(DYNAMIC_PARAM_PREDICATE, false);

        Iterator<SqlNode> paramIterator = request.getParams().iterator();
        for (SqlTreeNode dynamicNode : dynamicNodes) {
            SqlNode param;
            if (!paramIterator.hasNext()) {
                paramIterator = request.getParams().iterator();
            }
            param = paramIterator.next();
            dynamicNode.getSqlNodeSetter().accept(param);
        }
        if (paramIterator.hasNext()) {
            throw new DtmException("The number of passed parameters and parameters in the template does not match");
        } else {
            return selectTree.getRoot().getNode();
        }
    }

    @Override
    public QueryTemplateResult extract(SqlNode sqlNode) {
        return extract(sqlNode, Collections.emptyList());
    }

    @Override
    public QueryTemplateResult extract(SqlNode sqlNode, List<String> excludeColumns) {
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        List<SqlNode> params = setDynamicParams(excludeColumns, selectTree);
        SqlNode resultTemplateNode = selectTree.getRoot().getNode();
        return new QueryTemplateResult(
                resultTemplateNode
                        .toSqlString(sqlDialect).toString(),
                resultTemplateNode,
                params
        );
    }

    private List<SqlNode> setDynamicParams(List<String> excludeList, SqlSelectTree selectTree) {
        if (excludeList.isEmpty()) {
            return selectTree.findNodes(SET_DYNAMIC_PARAM, true).stream()
                    .flatMap(this::replace)
                    .collect(Collectors.toList());
        } else {
            return selectTree.findNodes(SET_DYNAMIC_PARAM, true).stream()
                    .flatMap(sqlTreeNode -> replaceWithExclude(sqlTreeNode, excludeList))
                    .collect(Collectors.toList());
        }
    }

    private Stream<SqlNode> replace(SqlTreeNode sqlTreeNode) {
        SqlNode sqlNode = sqlTreeNode.getNode();
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = sqlTreeNode.getNode();
            if (sqlBasicCall.getOperator() instanceof SqlInOperator) {
                return inReplace(sqlTreeNode, sqlBasicCall);
            } else if (sqlBasicCall.getOperator() instanceof SqlBetweenOperator) {
                return betweenReplace(sqlTreeNode, sqlBasicCall);
            } else if (sqlBasicCall.getOperands().length == 2) {
                return binaryReplace(sqlTreeNode, sqlBasicCall);
            }
        } else if (sqlNode instanceof SqlDynamicParam) {
            return Stream.of(sqlNode);
        }
        return Stream.empty();
    }

    private Stream<SqlNode> binaryReplace(SqlTreeNode sqlTreeNode, SqlBasicCall sqlBasicCall) {
        SqlNode leftOperand = sqlBasicCall.getOperands()[0];
        SqlNode rightOperand = sqlBasicCall.getOperands()[1];
        if ((leftOperand instanceof SqlIdentifier || isCollate(leftOperand)) && isValue(rightOperand)) {
            sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                    sqlBasicCall.getOperator(),
                    new SqlNode[]{leftOperand, DYNAMIC_PARAM},
                    sqlBasicCall.getParserPosition()
            ));
            return Stream.of(rightOperand);
        } else if (leftOperand instanceof SqlIdentifier && isCollate(rightOperand)) {
            SqlBasicCall rightBasicCall = (SqlBasicCall) rightOperand;
            SqlBasicCall newCollate = new SqlBasicCall(rightBasicCall.getOperator(),
                    new SqlNode[]{DYNAMIC_PARAM, rightBasicCall.getOperands()[1]},
                    rightBasicCall.getParserPosition());
            sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                    sqlBasicCall.getOperator(),
                    new SqlNode[]{leftOperand, newCollate},
                    sqlBasicCall.getParserPosition()
            ));
            return Stream.of(rightBasicCall.getOperands()[0]);
        } else if (isValue(leftOperand) && rightOperand instanceof SqlIdentifier) {
            sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                    sqlBasicCall.getOperator(),
                    new SqlNode[]{DYNAMIC_PARAM, rightOperand},
                    sqlBasicCall.getParserPosition()
            ));
            return Stream.of(leftOperand);
        }
        return Stream.empty();
    }

    private boolean isCollate(SqlNode operand) {
        if (!(operand instanceof SqlBasicCall))
            return false;
        SqlBasicCall basicCall = (SqlBasicCall) operand;
        return COLLATE_OPERATOR.equals(basicCall.getOperator().getName());
    }

    private boolean isValue(SqlNode rightOperand) {
        return rightOperand.getKind() == SqlKind.DYNAMIC_PARAM || rightOperand instanceof SqlLiteral;
    }

    private Stream<SqlNode> replaceWithExclude(SqlTreeNode sqlTreeNode, List<String> excludeList) {
        SqlNode sqlNode = sqlTreeNode.getNode();
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = sqlTreeNode.getNode();
            if (sqlBasicCall.getOperands().length == 2) {
                SqlNode leftOperand = sqlBasicCall.getOperands()[0];
                SqlNode rightOperand = sqlBasicCall.getOperands()[1];
                if (leftOperand instanceof SqlIdentifier &&
                        isValue(rightOperand) &&
                        isNotExclude(leftOperand, excludeList)) {
                    sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                            sqlBasicCall.getOperator(),
                            new SqlNode[]{leftOperand, DYNAMIC_PARAM},
                            sqlBasicCall.getParserPosition()
                    ));
                    return Stream.of(rightOperand);
                } else if (isValue(leftOperand) &&
                        rightOperand instanceof SqlIdentifier &&
                        isNotExclude(rightOperand, excludeList)) {
                    sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                            sqlBasicCall.getOperator(),
                            new SqlNode[]{DYNAMIC_PARAM, rightOperand},
                            sqlBasicCall.getParserPosition()
                    ));
                    return Stream.of(leftOperand);
                }
            } else if (sqlBasicCall.getOperator() instanceof SqlBetweenOperator) {
                return betweenReplace(sqlTreeNode, sqlBasicCall);
            }
        }
        return Stream.empty();
    }

    private Stream<SqlNode> betweenReplace(SqlTreeNode sqlTreeNode, SqlBasicCall sqlBasicCall) {
        SqlNode id = sqlBasicCall.getOperands()[0];
        SqlNode leftOperand = sqlBasicCall.getOperands()[1];
        List<SqlNode> params = new ArrayList<>();
        if (isValue(leftOperand)) {
            params.add(sqlBasicCall.getOperands()[1]);
            leftOperand = DYNAMIC_PARAM;
        }
        SqlNode rightOperand = sqlBasicCall.getOperands()[2];
        if (isValue(rightOperand)) {
            params.add(sqlBasicCall.getOperands()[2]);
            rightOperand = DYNAMIC_PARAM;
        }
        sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                sqlBasicCall.getOperator(),
                new SqlNode[]{id, leftOperand, rightOperand},
                sqlBasicCall.getParserPosition()
        ));
        return params.stream();
    }

    private Stream<SqlNode> inReplace(SqlTreeNode sqlTreeNode, SqlBasicCall sqlBasicCall) {
        SqlNode id = sqlBasicCall.getOperands()[0];
        if (sqlBasicCall.getOperands()[1] instanceof SqlNodeList) {
            SqlNodeList inList = (SqlNodeList) sqlBasicCall.getOperands()[1];

            SqlNodeList replacedNodeList = new SqlNodeList(inList.getList().stream()
                    .map(n -> DYNAMIC_PARAM)
                    .collect(Collectors.toList()), inList.getParserPosition());

            sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                    sqlBasicCall.getOperator(),
                    new SqlNode[]{id, replacedNodeList},
                    sqlBasicCall.getParserPosition()
            ));
            return inList.getList().stream();
        } else {
            return Stream.empty();
        }
    }

    private boolean isNotExclude(SqlNode operand, List<String> excludeList) {
        if (excludeList.isEmpty()) {
            return true;
        } else if (operand instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) operand;
            String columnName = identifier.isSimple() ? identifier.getSimple() : identifier.names.get(1);
            return excludeList.stream()
                    .noneMatch(e -> e.equalsIgnoreCase(columnName));
        } else {
            return true;
        }
    }

}
