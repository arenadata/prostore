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
package io.arenadata.dtm.query.execution.core.base.utils;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.apache.calcite.sql.type.SqlTypeName;

public class ColumnTypeUtil {
    public static ColumnType valueOf(SqlTypeName type) {
        switch (type) {
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case VARCHAR:
                return ColumnType.VARCHAR;
            case CHAR:
                return ColumnType.CHAR;
            case BIGINT:
                return ColumnType.BIGINT;
            case INTEGER:
                return ColumnType.INT;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case FLOAT:
                return ColumnType.FLOAT;
            case DATE:
                return ColumnType.DATE;
            case TIME:
                return ColumnType.TIME;
            case TIMESTAMP:
                return ColumnType.TIMESTAMP;
            default:
                return ColumnType.ANY;
        }
    }
}
