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
package io.arenadata.dtm.query.execution.plugin.adg.model.metadata;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public class ColumnTypeUtil {

    public static ColumnType columnTypeFromTtColumnType(String columnType) {
        switch (columnType) {
            case "unsigned":
            case "integer":
                return ColumnType.BIGINT;
            case "number":
                return ColumnType.DOUBLE;
            case "boolean":
                return ColumnType.BOOLEAN;
            case "string":
                return ColumnType.VARCHAR;
            default:
                return ColumnType.ANY;
        }
    }
}
