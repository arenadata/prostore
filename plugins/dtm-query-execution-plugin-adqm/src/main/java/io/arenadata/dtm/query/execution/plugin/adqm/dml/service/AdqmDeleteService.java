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
package io.arenadata.dtm.query.execution.plugin.adqm.dml.service;

import io.arenadata.dtm.calcite.adqm.extension.dml.SqlFinalTable;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicatePart;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicates;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import io.arenadata.dtm.query.execution.plugin.adqm.dml.factory.AdqmDmlSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.request.DeleteRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DeleteService;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

import static io.arenadata.dtm.query.calcite.core.util.SqlNodeTemplates.*;
import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.SYS_FROM_FIELD;
import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.SYS_TO_FIELD;
import static io.arenadata.dtm.query.execution.plugin.adqm.dml.util.AdqmLlwUtils.*;

@Service("adqmDeleteService")
public class AdqmDeleteService implements DeleteService {
    private static final SqlLiteral ONE_SYS_OP_LITERAL = longLiteral(1L);
    private static final SqlPredicates IDENTIFIER_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eq(SqlKind.IDENTIFIER))
            .build();

    private final AdqmDmlSqlFactory adqmDmlSqlFactory;
    private final DatabaseExecutor databaseExecutor;

    public AdqmDeleteService(AdqmDmlSqlFactory adqmDmlSqlFactory,
                             @Qualifier("adqmQueryExecutor") DatabaseExecutor databaseExecutor) {
        this.adqmDmlSqlFactory = adqmDmlSqlFactory;
        this.databaseExecutor = databaseExecutor;
    }

    @Override
    public Future<Void> execute(DeleteRequest request) {
        return Future.future(promise -> {
            val columns = getEntityColumnsList(request.getEntity());
            val source = prepareCloseSelect(request, request.getQuery().getCondition());
            // hack (source is EMPTY) because calcite adding braces to select 'INSERT INTO ... ( SELECT ... )' and ADQM can't handle that
            val resultInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, getActualTableIdentifier(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName()), SqlNodeList.EMPTY, columns);
            val insertSql = adqmDmlSqlFactory.getSqlFromNodes(resultInsert, source).replace(ARRAY_JOIN_PLACEHOLDER, ARRAY_JOIN_REPLACE);
            databaseExecutor.executeWithParams(insertSql, request.getParameters(), Collections.emptyList())
                    .compose(ignored -> databaseExecutor.executeUpdate(adqmDmlSqlFactory.getFlushSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName())))
                    .compose(ignored -> databaseExecutor.executeUpdate(adqmDmlSqlFactory.getOptimizeSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName())))
                    .onSuccess(ignored -> promise.complete())
                    .onFailure(promise::fail);
        });
    }

    private SqlSelect prepareCloseSelect(DeleteRequest request, SqlNode deleteCondition) {
        val selectList = getSelectListForClose(request.getEntity(), request.getSysCn(), ONE_SYS_OP_LITERAL);
        val actualTableIdentifier = getActualTableIdentifier(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName());
        val tableName = new SqlFinalTable(SqlParserPos.ZERO, as(actualTableIdentifier, TEMP_TABLE));
        val changedCondition = addTempTableToColumnsIdentifiers(deleteCondition);
        val sysFromLessThanCn = basicCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, identifier(TEMP_TABLE, SYS_FROM_FIELD), longLiteral(request.getDeltaOkSysCn()));
        val conditionAndFrom = getConditionAndFromCall(changedCondition, sysFromLessThanCn);
        val sysToMoreThanCn = basicCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, identifier(TEMP_TABLE, SYS_TO_FIELD), longLiteral(request.getDeltaOkSysCn()));
        val actualWhere = basicCall(SqlStdOperatorTable.AND, conditionAndFrom, sysToMoreThanCn);
        return new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList, tableName, actualWhere, null, null, SqlNodeList.EMPTY, null, null, null, SqlNodeList.EMPTY);
    }

    private SqlBasicCall getConditionAndFromCall(SqlNode changedCondition, SqlBasicCall sysFromLessThanCn) {
        if (changedCondition == null) {
            return sysFromLessThanCn;
        }

        return basicCall(SqlStdOperatorTable.AND, changedCondition, sysFromLessThanCn);
    }

    private SqlNode addTempTableToColumnsIdentifiers(SqlNode deleteCondition) {
        if (deleteCondition == null) {
            return null;
        }

        val changedCondition = SqlNodeUtil.copy(deleteCondition);
        new SqlSelectTree(changedCondition).findNodes(IDENTIFIER_PREDICATE, false)
                .forEach(sqlTreeNode -> {
                    if (sqlTreeNode.getNode() instanceof SqlIdentifier) {
                        List<String> names = ((SqlIdentifier) sqlTreeNode.getNode()).names;
                        sqlTreeNode.getSqlNodeSetter().accept(identifier(TEMP_TABLE, names.get(names.size() - 1)));
                    }
                });
        return changedCondition;
    }
}
