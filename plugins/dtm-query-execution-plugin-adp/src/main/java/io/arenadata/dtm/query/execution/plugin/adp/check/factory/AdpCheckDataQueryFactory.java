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
package io.arenadata.dtm.query.execution.plugin.adp.check.factory;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adp.base.Constants;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import lombok.val;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AdpCheckDataQueryFactory {

    private AdpCheckDataQueryFactory() {
    }

    public static final String COUNT_COLUMN_NAME = "cnt";
    public static final String HASH_SUM_COLUMN_NAME = "hash_sum";
    private static final String CHECK_DATA_BY_COUNT_TEMPLATE = "SELECT count(1) as %s FROM " +
            "(SELECT 1 " +
            "FROM %s.%s_%s " +
            "WHERE (sys_to = %d AND sys_op = 1) OR sys_from = %d) AS tmp";

    private static final String CHECK_DATA_BY_HASH_TEMPLATE =
            "SELECT sum(dtmInt32Hash(MD5(concat(%s))::bytea)) as %s FROM\n" +
                    "(\n" +
                    "  SELECT %s \n" +
                    "  FROM %s.%s_%s \n" +
                    "  WHERE (sys_to = %d AND sys_op = 1) OR sys_from = %d) AS tmp";

    public static String createCheckDataByCountQuery(CheckDataByCountRequest request) {
        return String.format(CHECK_DATA_BY_COUNT_TEMPLATE,
                COUNT_COLUMN_NAME,
                request.getEntity().getSchema(),
                request.getEntity().getName(),
                Constants.ACTUAL_TABLE,
                request.getSysCn() - 1,
                request.getSysCn());
    }

    public static String createCheckDataByHashInt32Query(CheckDataByHashInt32Request request) {
        Map<String, EntityField> fields = request.getEntity().getFields().stream()
                .collect(Collectors.toMap(EntityField::getName, Function.identity()));
        val fieldsConcatenationList = request.getColumns().stream()
                .map(fields::get)
                .map(AdpCheckDataQueryFactory::create)
                .collect(Collectors.joining(",';',"));
        val columnsList = String.join(",';',", request.getColumns());
        val datamart = request.getEntity().getSchema();
        val table = request.getEntity().getName();
        val sysCn = request.getSysCn();
        return String.format(CHECK_DATA_BY_HASH_TEMPLATE,
                fieldsConcatenationList,
                HASH_SUM_COLUMN_NAME,
                columnsList,
                datamart,
                table,
                Constants.ACTUAL_TABLE,
                sysCn - 1,
                sysCn);
    }

    private static String create(EntityField field) {
        String result;
        switch (field.getType()) {
            case BOOLEAN:
                result = String.format("%s::int", field.getName());
                break;
            case DATE:
                result = String.format("%s - make_date(1970, 01, 01)", field.getName());
                break;
            case TIME:
            case TIMESTAMP:
                result = String.format("(extract(epoch from %s)*1000000)::bigint", field.getName());
                break;
            default:
                result = field.getName();
        }
        return result;
    }
}
