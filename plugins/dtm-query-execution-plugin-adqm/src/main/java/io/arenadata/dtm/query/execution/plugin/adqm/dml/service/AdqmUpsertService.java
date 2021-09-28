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
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeTemplates;
import io.arenadata.dtm.query.execution.plugin.adqm.dml.factory.AdqmDmlSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dml.LlwUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.UpsertRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import io.arenadata.dtm.query.execution.plugin.api.service.UpsertService;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.arenadata.dtm.query.calcite.core.util.SqlNodeTemplates.*;
import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.SYS_FROM_FIELD;
import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.SYS_TO_FIELD;
import static io.arenadata.dtm.query.execution.plugin.adqm.dml.util.AdqmLlwUtils.*;

@Service("adqmUpsertService")
public class AdqmUpsertService implements UpsertService {
    private static final SqlLiteral ZERO_SYS_OP_LITERAL = longLiteral(0L);
    private static final SqlLiteral ONE_SIGN_LITERAL = longLiteral(1L);

    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final AdqmDmlSqlFactory adqmDmlSqlFactory;
    private final DatabaseExecutor databaseExecutor;

    public AdqmUpsertService(@Qualifier("adqmTemplateParameterConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                             AdqmDmlSqlFactory adqmDmlSqlFactory,
                             @Qualifier("adqmQueryExecutor") DatabaseExecutor databaseExecutor) {
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.adqmDmlSqlFactory = adqmDmlSqlFactory;
        this.databaseExecutor = databaseExecutor;
    }

    @Override
    public Future<Void> execute(UpsertRequest request) {
        return Future.future(promise -> {
            val source = (SqlCall) request.getQuery().getSource();

            val logicalFields = LlwUtils.getFilteredLogicalFields(request.getEntity(), request.getQuery().getTargetColumnList());
            val pkFieldNames = EntityFieldUtils.getPkFieldNames(request.getEntity());
            validatePrimaryKeys(logicalFields, pkFieldNames);

            val systemRowValuesToAdd = Arrays.asList(longLiteral(request.getSysCn()), MAX_CN_LITERAL,
                    ZERO_SYS_OP_LITERAL, MAX_CN_LITERAL, ONE_SIGN_LITERAL);
            val actualValues = LlwUtils.getExtendRowsOfValues(source, logicalFields, systemRowValuesToAdd,
                    transformEntry -> pluginSpecificLiteralConverter.convert(transformEntry.getSqlNode(), transformEntry.getSqlTypeName()));
            val pkValuesRows = getPrimaryKeyValues(logicalFields, actualValues);

            val actualInsertSql = getSqlInsert(request, logicalFields, actualValues);
            val closeInsertSql = prepareCloseInsert(request, pkValuesRows, pkFieldNames);
            val pkParams = getPrimaryKeyParams(request.getParameters(), source, logicalFields);

            databaseExecutor.executeWithParams(actualInsertSql, request.getParameters(), Collections.emptyList())
                    .compose(ignored -> databaseExecutor.executeWithParams(closeInsertSql, pkParams, Collections.emptyList()))
                    .compose(ignored -> databaseExecutor.executeUpdate(adqmDmlSqlFactory.getFlushSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName())))
                    .compose(ignored -> databaseExecutor.executeUpdate(adqmDmlSqlFactory.getOptimizeSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName())))
                    .onComplete(promise);
        });
    }

    private QueryParameters getPrimaryKeyParams(QueryParameters queryParameters, SqlCall valuesNode, List<EntityField> logicalFields) {
        if (queryParameters == null) {
            return null;
        }
        val origValues = queryParameters.getValues();
        val origTypes = queryParameters.getTypes();
        List<Object> newValues = new ArrayList<>();
        List<ColumnType> newTypes = new ArrayList<>();

        int dynamicParamIndex = 0;
        for (SqlNode sqlNode : valuesNode.getOperandList()) {
            val sqlCall = (SqlCall) sqlNode;
            val operandList = sqlCall.getOperandList();

            for (int i = 0; i < operandList.size(); i++) {
                val toTransform = operandList.get(i);
                if (toTransform instanceof SqlDynamicParam) {
                    if (logicalFields.get(i).getPrimaryOrder() != null) {
                        newValues.add(origValues.get(dynamicParamIndex));
                        newTypes.add(origTypes.get(dynamicParamIndex));
                    }
                    dynamicParamIndex++;
                }
            }
        }

        return new QueryParameters(newValues, newTypes);
    }

    private SqlNodeList getPrimaryKeyValues(List<EntityField> logicalFields, SqlBasicCall valuesNode) {
        val pkValuesRows = new SqlNodeList(SqlParserPos.ZERO);
        for (SqlNode operand : valuesNode.operands) {
            val rowNode = (SqlBasicCall) operand;
            val primaryKeyRowValues = new ArrayList<SqlNode>();
            for (int i = 0; i < logicalFields.size(); i++) {
                val entityField = logicalFields.get(i);
                if (entityField.getPrimaryOrder() != null) {
                    primaryKeyRowValues.add(rowNode.operand(i));
                }
            }
            pkValuesRows.add(basicCall(SqlStdOperatorTable.ROW, primaryKeyRowValues));
        }
        return pkValuesRows;
    }

    private String prepareCloseInsert(UpsertRequest request, SqlNodeList pkValuesRows, List<String> pkFieldNames) {
        val columns = getEntityColumnsList(request.getEntity());
        val source = prepareCloseSelect(request, pkValuesRows, pkFieldNames);
        // hack (source is EMPTY) because calcite adding braces to select 'INSERT INTO ... ( SELECT ... )' and ADQM can't handle that
        val insert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, getActualTableIdentifier(request), new SqlNodeList(SqlParserPos.ZERO), columns);
        return adqmDmlSqlFactory.getSqlFromNodes(insert, source).replace(ARRAY_JOIN_PLACEHOLDER, ARRAY_JOIN_REPLACE);
    }

    private String getSqlInsert(UpsertRequest request, List<EntityField> insertedColumns, SqlBasicCall
            actualValues) {
        val actualColumnList = getInsertedColumnsList(insertedColumns);
        val result = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, getActualTableIdentifier(request), actualValues, actualColumnList);
        return adqmDmlSqlFactory.getSqlFromNodes(result);
    }

    private SqlSelect prepareCloseSelect(UpsertRequest request, SqlNodeList pkValuesRows, List<String> pkFields) {
        val selectList = getSelectListForClose(request.getEntity(), request.getSysCn(), ZERO_SYS_OP_LITERAL);
        val actualTableIdentifier = getActualTableIdentifier(request);
        val tableName = new SqlFinalTable(SqlParserPos.ZERO, as(actualTableIdentifier, TEMP_TABLE));
        val pkFieldsIdentifiers = pkFields.stream()
                .map(SqlNodeTemplates::identifier)
                .toArray(SqlNode[]::new);
        val pkKeysRow = basicCall(SqlStdOperatorTable.ROW, pkFieldsIdentifiers);
        val inCall = basicCall(SqlStdOperatorTable.IN, pkKeysRow, pkValuesRows);
        val sysFromLessThanCn = basicCall(SqlStdOperatorTable.LESS_THAN, identifier(TEMP_TABLE, SYS_FROM_FIELD), longLiteral(request.getSysCn()));
        val sysToMoreThanCn = basicCall(SqlStdOperatorTable.GREATER_THAN, identifier(TEMP_TABLE, SYS_TO_FIELD), longLiteral(request.getSysCn()));
        val inAndFrom = basicCall(SqlStdOperatorTable.AND, inCall, sysFromLessThanCn);
        val actualWhere = basicCall(SqlStdOperatorTable.AND, inAndFrom, sysToMoreThanCn);

        return new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList, tableName, actualWhere, null, null, SqlNodeList.EMPTY, null, null, null, SqlNodeList.EMPTY);
    }

    private void validatePrimaryKeys(List<EntityField> insertedColumns, List<String> pkFieldNames) {
        List<String> notFoundPrimaryKeys = new ArrayList<>(pkFieldNames.size());
        for (String pkFieldName : pkFieldNames) {
            boolean containsInColumns = insertedColumns.stream().anyMatch(entityField -> entityField.getName().equals(pkFieldName));
            if (!containsInColumns) {
                notFoundPrimaryKeys.add(pkFieldName);
            }
        }

        if (!notFoundPrimaryKeys.isEmpty()) {
            throw new DtmException(String.format("Inserted values must contain primary keys: %s", notFoundPrimaryKeys));
        }
    }

    private SqlNodeList getInsertedColumnsList(List<EntityField> insertedColumns) {
        val columnList = new SqlNodeList(SqlParserPos.ZERO);
        for (EntityField insertedColumn : insertedColumns) {
            columnList.add(identifier(insertedColumn.getName()));
        }
        fillSystemColumns(columnList);
        return columnList;
    }

    private SqlIdentifier getActualTableIdentifier(UpsertRequest request) {
        return identifier(String.format("%s__%s", request.getEnvName(), request.getDatamartMnemonic()), String.format("%s_actual", request.getEntity().getName()));
    }
}
