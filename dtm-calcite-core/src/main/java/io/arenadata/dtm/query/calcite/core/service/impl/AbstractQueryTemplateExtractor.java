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
    public static final String DYNAMIC_PARAM_PATH = ".DYNAMIC_PARAM";
    private static final SqlDynamicParam DYNAMIC_PARAM = new SqlDynamicParam(0, SqlParserPos.QUOTED_ZERO);
    private static final String REGEX = "(?i).*(LIKE|EQUAL\\w*|LESS\\w*|GREATER\\w*|BETWEEN\\w*|.IN\\w*)";
    private final DefinitionService<SqlNode> definitionService;
    private final SqlDialect sqlDialect;

    public AbstractQueryTemplateExtractor(DefinitionService<SqlNode> definitionService, SqlDialect sqlDialect) {
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
        List<SqlTreeNode> dynamicNodes = selectTree.findNodesByPath(DYNAMIC_PARAM_PATH);

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
        return extractTemplate(sqlNode, Collections.emptyList());
    }

    @Override
    public QueryTemplateResult extract(SqlNode sqlNode, List<String> excludeColumns) {
        return extractTemplate(sqlNode, excludeColumns);
    }

    private QueryTemplateResult extractTemplate(SqlNode sqlNode, List<String> excludeList) {
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        List<SqlNode> params = setDynamicParams(excludeList, selectTree);
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
            return selectTree.findNodesByPathRegex(REGEX).stream()
                    .flatMap(this::replace)
                    .collect(Collectors.toList());
        } else {
            return selectTree.findNodesByPathRegex(REGEX).stream()
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
            } else if (sqlBasicCall.getOperands().length == 2) {
                SqlNode leftOperand = sqlBasicCall.getOperands()[0];
                SqlNode rightOperand = sqlBasicCall.getOperands()[1];
                if (leftOperand instanceof SqlIdentifier && rightOperand instanceof SqlLiteral) {
                    sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                            sqlBasicCall.getOperator(),
                            new SqlNode[]{leftOperand, DYNAMIC_PARAM},
                            sqlBasicCall.getParserPosition()
                    ));
                    return Stream.of(rightOperand);
                } else if (leftOperand instanceof SqlLiteral && rightOperand instanceof SqlIdentifier) {
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

    private Stream<SqlNode> replaceWithExclude(SqlTreeNode sqlTreeNode, List<String> excludeList) {
        SqlNode sqlNode = sqlTreeNode.getNode();
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = sqlTreeNode.getNode();
            if (sqlBasicCall.getOperands().length == 2) {
                SqlNode leftOperand = sqlBasicCall.getOperands()[0];
                SqlNode rightOperand = sqlBasicCall.getOperands()[1];
                if (leftOperand instanceof SqlIdentifier &&
                        rightOperand instanceof SqlLiteral &&
                        isNotExclude(leftOperand, excludeList)) {
                    sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                            sqlBasicCall.getOperator(),
                            new SqlNode[]{leftOperand, DYNAMIC_PARAM},
                            sqlBasicCall.getParserPosition()
                    ));
                    return Stream.of(rightOperand);
                } else if (leftOperand instanceof SqlLiteral &&
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
        if (leftOperand instanceof SqlLiteral) {
            params.add(sqlBasicCall.getOperands()[1]);
            leftOperand = DYNAMIC_PARAM;
        }
        SqlNode rightOperand = sqlBasicCall.getOperands()[2];
        if (rightOperand instanceof SqlLiteral) {
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
