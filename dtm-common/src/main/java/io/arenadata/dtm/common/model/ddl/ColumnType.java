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
package io.arenadata.dtm.common.model.ddl;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Logical model data types
 */
public enum ColumnType {
    VARCHAR(Types.VARCHAR, String.class, new String[]{"varchar"}),
    CHAR(Types.CHAR, String.class, new String[]{"char"}),
    BIGINT(Types.BIGINT, Long.class, new String[]{"bigint"}),
    INT(Types.INTEGER, Long.class, new String[]{"int", "integer"}),
    INT32(Types.INTEGER, Integer.class, new String[]{"int32"}),
    DOUBLE(Types.DOUBLE, Double.class, new String[]{"double"}),
    FLOAT(Types.FLOAT, Float.class, new String[]{"float"}),
    DATE(Types.DATE, Date.class, new String[]{"date"}),
    TIME(Types.TIME, Time.class, new String[]{"time"}),
    TIMESTAMP(Types.TIMESTAMP, Timestamp.class, new String[]{"timestamp"}),
    BOOLEAN(Types.BOOLEAN, Boolean.class, new String[]{"boolean"}),
    BLOB(Types.BLOB, Object.class, new String[]{"blob"}),
    UUID(Types.OTHER, String.class, new String[]{"uuid"}),
    ANY(Types.OTHER, Object.class, new String[]{"any"});

    private final int sqlType;
    private final String[] aliases;
    private final Class<?> clazz;

    ColumnType(int sqlType, Class<?> clazz, String[] aliases) {
        this.sqlType = sqlType;
        this.aliases = aliases;
        this.clazz = clazz;
    }

    public static ColumnType fromTypeString(String typeString) {
        String s = typeString.trim();
        ColumnType[] types = values();
        for (ColumnType dataType : types) {
            if (s.equalsIgnoreCase(dataType.name())) {
                return dataType;
            }
            for (int j = 0; j < dataType.aliases.length; ++j) {
                String alias = dataType.aliases[j];
                if (s.equalsIgnoreCase(alias)) {
                    return dataType;
                }
            }
        }
        return ANY;
    }

    public int getSqlType() {
        return sqlType;
    }

    public String[] getAliases() {
        return aliases;
    }

    public Class<?> getClazz() {
        return clazz;
    }
}
