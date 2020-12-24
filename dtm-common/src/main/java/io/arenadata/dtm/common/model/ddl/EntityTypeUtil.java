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
package io.arenadata.dtm.common.model.ddl;

import java.util.Optional;

public class EntityTypeUtil {
    public static String pgFromDtmType(EntityField field) {
        return pgFromDtmType(field.getType(), field.getSize(), field.getAccuracy());
    }

    public static String pgFromDtmType(ColumnType type, Integer size, Integer accuracy) {
        switch (type) {
            case DATE:
                return "date";
            case TIME:
                return "time" + getTimePrecision(accuracy);
            case TIMESTAMP:
                return "timestamp" + getTimestamprecision(accuracy);
            case FLOAT:
                return "float4";
            case DOUBLE:
                return "float8";
            case BOOLEAN:
                return "bool";
            case INT:
            case BIGINT:
                return "int8";
            case CHAR:
            case VARCHAR:
                return "varchar" + getVarcharSize(size);
            case UUID:
                return "varchar(36)";
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type: %s", type));
        }
    }

    private static String getTimestamprecision(Integer accuracy) {
        return getTimePrecision(Optional.ofNullable(accuracy).orElse(6));
    }

    private static String getTimePrecision(Integer accuracy) {
        return Optional.ofNullable(accuracy)
                .map(accuracyVal -> String.format("(%s)", accuracyVal))
                .orElse("");
    }

    private static String getVarcharSize(Integer size) {
        return Optional.ofNullable(size)
                .map(sizeVal -> String.format("(%s)", sizeVal))
                .orElse("");
    }
}
