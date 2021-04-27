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
package io.arenadata.dtm.query.execution.plugin.adb.check.factory.impl;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbCheckDataByHashFieldValueFactory;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbCheckDataQueryFactory;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import lombok.val;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AdbCheckDataWithHistoryFactory implements AdbCheckDataQueryFactory {

    private final AdbCheckDataByHashFieldValueFactory checkDataByHashFieldValueFactory;

    private static final String CHECK_DATA_BY_COUNT_TEMPLATE = "SELECT count(1) as %s FROM " +
            "(SELECT 1 " +
            "FROM %s.%s_%s " +
            "WHERE (sys_to = %d AND sys_op = 1) OR sys_from = %d" +
            "UNION ALL " +
            "SELECT 1 " +
            "FROM %s.%s_%s " +
            "WHERE sys_from = %d) AS tmp";

    private static final String CHECK_DATA_BY_HASH_TEMPLATE =
            "SELECT sum(dtmInt32Hash(MD5(concat(%s))::bytea)) as %s FROM\n" +
                    "(\n" +
                    "  SELECT %s \n" +
                    "  FROM %s.%s_%s \n" +
                    "  WHERE (sys_to = %d AND sys_op = 1) OR sys_from = %d \n" +
                    "  UNION ALL \n" +
                    "  SELECT %s \n" +
                    "  FROM %s.%s_%s \n" +
                    "  WHERE sys_from = %d\n" +
                    ") AS tmp";

    public AdbCheckDataWithHistoryFactory(AdbCheckDataByHashFieldValueFactory checkDataByHashFieldValueFactory) {
        this.checkDataByHashFieldValueFactory = checkDataByHashFieldValueFactory;
    }


    @Override
    public String createCheckDataByCountQuery(CheckDataByCountRequest request, String resultColumnName) {
        return String.format(CHECK_DATA_BY_COUNT_TEMPLATE,
                resultColumnName,
                request.getEntity().getSchema(),
                request.getEntity().getName(),
                Constants.HISTORY_TABLE,
                request.getSysCn() - 1,
                request.getSysCn(),
                request.getEntity().getSchema(),
                request.getEntity().getName(),
                Constants.ACTUAL_TABLE,
                request.getSysCn());
    }

    @Override
    public String createCheckDataByHashInt32Query(CheckDataByHashInt32Request request, String resultColumnName) {
        Map<String, EntityField> fields = request.getEntity().getFields().stream()
                .collect(Collectors.toMap(EntityField::getName, Function.identity()));
        val fieldsConcatenationList = request.getColumns().stream()
                .map(fields::get)
                .map(checkDataByHashFieldValueFactory::create)
                .collect(Collectors.joining(",';',"));
        val columnsList = String.join(",';',", request.getColumns());
        val datamart = request.getEntity().getSchema();
        val table = request.getEntity().getName();
        val sysCn = request.getSysCn();
        return String.format(CHECK_DATA_BY_HASH_TEMPLATE,
                fieldsConcatenationList,
                resultColumnName,
                columnsList,
                datamart,
                table,
                Constants.HISTORY_TABLE,
                sysCn - 1,
                sysCn,
                columnsList,
                datamart,
                table,
                Constants.ACTUAL_TABLE,
                sysCn);
    }
}
