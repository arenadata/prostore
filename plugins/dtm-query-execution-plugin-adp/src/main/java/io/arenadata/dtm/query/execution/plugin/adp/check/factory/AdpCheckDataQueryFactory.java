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
import io.arenadata.dtm.query.execution.plugin.api.check.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckDataByHashInt32Request;
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
            "WHERE (sys_from >= %d AND sys_from <= %d )\n" +
            " OR\n" +
            " (COALESCE(sys_to, 9223372036854775807) >= %d AND COALESCE(sys_to, 9223372036854775807) <= %d AND sys_op = 1)) AS tmp";

    private static final String CHECK_DATA_BY_HASH_TEMPLATE =
            "SELECT sum(dtmInt32Hash(MD5(concat(%s))::bytea)/%d) as %s FROM\n" +
                    " (\n" +
                    " SELECT %s\n" +
                    " FROM %s.%s_%s\n" +
                    " WHERE (sys_from >= %d AND sys_from <= %d )\n" +
                    " OR\n" +
                    " (COALESCE(sys_to, 9223372036854775807) >= %d AND COALESCE(sys_to, 9223372036854775807) <= %d AND sys_op = 1)) AS tmp";

    public static String createCheckDataByCountQuery(CheckDataByCountRequest request) {
        return String.format(CHECK_DATA_BY_COUNT_TEMPLATE,
                COUNT_COLUMN_NAME,
                request.getEntity().getSchema(),
                request.getEntity().getName(),
                Constants.ACTUAL_TABLE,
                request.getCnFrom(),
                request.getCnTo(),
                request.getCnFrom() - 1,
                request.getCnTo() - 1);
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
        val normalization = request.getNormalization();
        return String.format(CHECK_DATA_BY_HASH_TEMPLATE,
                fieldsConcatenationList,
                normalization,
                HASH_SUM_COLUMN_NAME,
                columnsList,
                datamart,
                table,
                Constants.ACTUAL_TABLE,
                request.getCnFrom(),
                request.getCnTo(),
                request.getCnFrom() - 1,
                request.getCnTo() - 1);
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
