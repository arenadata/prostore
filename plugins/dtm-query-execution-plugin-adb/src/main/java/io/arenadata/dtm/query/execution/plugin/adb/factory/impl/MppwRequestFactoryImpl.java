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
package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class MppwRequestFactoryImpl implements MppwRequestFactory {

    String INSERT_HISTORY_SQL = "INSERT INTO %s.%s_history (%s)\n" +
        "SELECT %s\n" +
        "FROM %s.%s_actual a\n" +
        "         INNER JOIN %s.%s_staging s ON\n" +
        "    %s";

    String DELETE_ACTUAL_SQL = "DELETE\n" +
        "FROM %s.%s_actual a USING %s.%s_staging s\n" +
        "WHERE %s";

    String INSERT_ACTUAL_SQL = "INSERT INTO %s.%s_actual (%s)\n" +
        "SELECT %s\n" +
        "FROM %s.%s_staging\n" +
        "WHERE %s.%s_staging.sys_op <> 1";

    String TRUNCATE_STAGING_SQL = "TRUNCATE %s.%s_staging";

    @Override
    public List<PreparedStatementRequest> create(MppwTransferDataRequest request) {
        String actualColumns = request.getColumnList().stream()
            .map(s -> "a." + s)
            .map(cn -> ("a.sys_to".equals(cn)) ? request.getHotDelta() - 1 + "" : cn)
            .map(cn -> ("a.sys_op".equals(cn)) ? "s.sys_op" : cn)
            .collect(Collectors.joining(","));
        String joinConditionInsert = request.getKeyColumnList().stream()
            .filter(columnName -> !MetadataSqlFactoryImpl.SYS_FROM_ATTR.equals(columnName))
            .map(key -> "s." + key + "=" + "a." + key)
            .collect(Collectors.joining(" AND "));

        String joinConditionDelete = request.getKeyColumnList().stream()
            .filter(columnName -> !MetadataSqlFactoryImpl.SYS_FROM_ATTR.equals(columnName))
            .map(key -> "a." + key + "=" + "s." + key)
            .collect(Collectors.joining(" AND "));

        String columnsString = String.join(",", request.getColumnList());

        String insertHistorySql = String.format(INSERT_HISTORY_SQL,
            request.getDatamart(), request.getTableName(), columnsString,
            actualColumns,
            request.getDatamart(), request.getTableName(),
            request.getDatamart(), request.getTableName(),
            joinConditionInsert);

        String deleteActualSql = String.format(DELETE_ACTUAL_SQL,
            request.getDatamart(), request.getTableName(), request.getDatamart(), request.getTableName(),
            joinConditionDelete);

        String stagingColumnsString = String.join(",", getStagingColumnList(request));

        String insertActualSql = String.format(INSERT_ACTUAL_SQL,
            request.getDatamart(), request.getTableName(), columnsString,
            stagingColumnsString,
            request.getDatamart(), request.getTableName(),
            request.getDatamart(), request.getTableName());

        String truncateStagingSql = String.format(TRUNCATE_STAGING_SQL,
            request.getDatamart(), request.getTableName());

        return Arrays.asList(
            PreparedStatementRequest.onlySql(insertHistorySql),
            PreparedStatementRequest.onlySql(deleteActualSql),
            PreparedStatementRequest.onlySql(insertActualSql),
            PreparedStatementRequest.onlySql(truncateStagingSql)
        );
    }

    private List<String> getStagingColumnList(MppwTransferDataRequest request) {
        return request.getColumnList().stream()
            .map(fieldName ->
                MetadataSqlFactoryImpl.SYS_FROM_ATTR.equals(fieldName) ? String.valueOf(request.getHotDelta()) : fieldName)
            .collect(Collectors.toList());
    }
}
