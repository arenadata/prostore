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
package io.arenadata.dtm.jdbc.core;

/**
 * Информация о поле в ResultSet
 */
public class Field {

    /**
     * Название колонки
     */
    private String columnLabel;

    /**
     * Значение в данном поле
     */
    private Object value;

    public Field() {
    }

    public Field(String columnLabel, Object value) {
        this.columnLabel = columnLabel;
        this.value = value;
    }

    public String getColumnLabel() {
        return columnLabel;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Field{" +
                "columnLabel='" + columnLabel + '\'' +
                ", value=" + value +
                '}';
    }
}
