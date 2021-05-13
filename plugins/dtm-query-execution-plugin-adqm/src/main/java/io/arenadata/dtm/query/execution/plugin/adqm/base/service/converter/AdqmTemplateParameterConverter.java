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
package io.arenadata.dtm.query.execution.plugin.adqm.base.service.converter;

import io.arenadata.dtm.query.execution.plugin.api.service.TemplateParameterConverter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

@Service("adqmTemplateParameterConverter")
public class AdqmTemplateParameterConverter implements TemplateParameterConverter {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .toFormatter();
    private static final ZoneId ZONE_ID = ZoneId.of("UTC");


    @Override
    public List<SqlNode> convert(List<SqlNode> params, List<SqlTypeName> parameterTypes) {
        List<SqlNode> nwParams = new ArrayList<>();
        for (int i = 0; i < params.size(); i++) {
            nwParams.add(convertParam(params.get(i), parameterTypes.get(i)));
        }
        return nwParams;
    }

    protected SqlNode convertParam(SqlNode param, SqlTypeName typeName) {
        if (SqlKind.DYNAMIC_PARAM.equals(param.getKind())) {
            return param;
        }
        switch (typeName) {
            case BOOLEAN:
                Boolean aBoolean = ((SqlLiteral) param).getValueAs(Boolean.class);
                if (aBoolean == null) {
                    return SqlLiteral.createNull(param.getParserPosition());
                } else {
                    return SqlLiteral.createExactNumeric(aBoolean ? "1" : "0", param.getParserPosition());
                }
            case TIME:
                LocalTime time = LocalTime.parse(((SqlLiteral) param).getValueAs(String.class));
                long nanoOfDay = time.toNanoOfDay();
                return SqlLiteral.createExactNumeric(String.valueOf(nanoOfDay / 1000), param.getParserPosition());
            case DATE:
                long epochDay = LocalDate.parse(((SqlLiteral) param).getValueAs(String.class), DateTimeFormatter.ISO_LOCAL_DATE)
                        .toEpochDay();
                return SqlLiteral.createExactNumeric(String.valueOf(epochDay), param.getParserPosition());
            case TIMESTAMP:
                LocalDateTime dateTime = LocalDateTime.parse(((SqlLiteral) param).getValueAs(String.class), TIMESTAMP_FORMATTER);
                Long transformTimestamp = transformTimestamp(dateTime);
                return SqlNumericLiteral.createExactNumeric(String.valueOf(transformTimestamp), param.getParserPosition());
            default:
                return param;
        }
    }

    public Long transformTimestamp(LocalDateTime value) {
        if (value != null) {
            Instant instant = getInstant(value);
            return instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND);
        }
        return null;
    }

    private Instant getInstant(LocalDateTime value) {
        return value.atZone(ZONE_ID).toInstant();
    }
}
