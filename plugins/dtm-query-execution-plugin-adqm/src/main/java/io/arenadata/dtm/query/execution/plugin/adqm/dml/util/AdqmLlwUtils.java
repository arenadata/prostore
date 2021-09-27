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
package io.arenadata.dtm.query.execution.plugin.adqm.dml.util;

import io.arenadata.dtm.common.configuration.core.CoreConstants;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.TimestampString;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;

import static io.arenadata.dtm.query.calcite.core.util.SqlNodeTemplates.identifier;
import static io.arenadata.dtm.query.calcite.core.util.SqlNodeTemplates.longLiteral;
import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.*;

public final class AdqmLlwUtils {
    public static final String TEMP_TABLE = "__a";
    public static final SqlNumericLiteral MAX_CN_LITERAL = SqlLiteral.createExactNumeric("9223372036854775807", SqlParserPos.ZERO);
    public static final String ARRAY_JOIN_PLACEHOLDER = "<$array_join$>";
    public static final String ARRAY_JOIN_REPLACE = "arrayJoin([-1, 1])";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private AdqmLlwUtils() {
    }

    public static void fillSystemColumns(SqlNodeList columnList) {
        columnList.add(identifier(SYS_FROM_FIELD));
        columnList.add(identifier(SYS_TO_FIELD));
        columnList.add(identifier(SYS_OP_FIELD));
        columnList.add(identifier(SYS_CLOSE_DATE_FIELD));
        columnList.add(identifier(SIGN_FIELD));
    }

    public static SqlNodeList getEntityColumnsList(Entity entity) {
        val columnList = new SqlNodeList(SqlParserPos.ZERO);
        entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .forEach(entityField -> columnList.add(identifier(entityField.getName())));
        fillSystemColumns(columnList);
        return columnList;
    }

    public static SqlIdentifier getActualTableIdentifier(String env, String datamart, String entityName) {
        return identifier(String.format("%s__%s", env, datamart), String.format("%s_actual", entityName));
    }

    public static SqlNodeList getSelectListForClose(Entity entity, long sysCn, SqlLiteral sysOpLiteral) {
        val selectList = new SqlNodeList(SqlParserPos.ZERO);
        entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .forEach(entityField -> selectList.add(identifier(TEMP_TABLE, entityField.getName())));
        selectList.add(identifier(TEMP_TABLE, SYS_FROM_FIELD));
        selectList.add(longLiteral(sysCn - 1));
        selectList.add(sysOpLiteral);

        val now = LocalDateTime.now(CoreConstants.CORE_ZONE_ID).format(DATE_TIME_FORMATTER);
        selectList.add(SqlLiteral.createTimestamp(new TimestampString(now), 0, SqlParserPos.ZERO));
        selectList.add(identifier(ARRAY_JOIN_PLACEHOLDER));
        return selectList;
    }
}
