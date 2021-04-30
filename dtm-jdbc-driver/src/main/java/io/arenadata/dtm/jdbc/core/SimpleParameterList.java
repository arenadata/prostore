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
package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.util.ColumnTypeUtil;
import io.arenadata.dtm.jdbc.util.DtmSqlException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SimpleParameterList implements ParameterList {

    private final List<Object> paramValues;
    private final List<ColumnType> paramTypes;
    private static final Object NULL_OBJECT = new Object();

    public SimpleParameterList() {
        this.paramValues = new ArrayList<>();
        this.paramTypes = new ArrayList<>();
    }

    private void bind(int index, Object value, int sqlType) throws DtmSqlException {
        if (index >= 0) {
            this.paramValues.add(value);
            this.paramTypes.add(ColumnTypeUtil.getColumnType(sqlType));
        } else {
            throw new DtmSqlException(String.format("The column index is out of range: %d, number of columns: %d",
                    index, this.paramValues.size()));
        }
    }

    @Override
    public void setNull(int index, int sqlType) throws SQLException {
        this.bind(index, NULL_OBJECT, sqlType);
    }

    @Override
    public void setBoolean(int index, boolean value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setByte(int index, byte value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setShort(int index, short value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setInt(int index, int value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setLong(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setFloat(int index, float value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setDouble(int index, double value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setBigDecimal(int index, BigDecimal value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setString(int index, String value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setBytes(int index, byte[] value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setDate(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setTime(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setTimestamp(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public ParameterList copy() {
        SimpleParameterList newCopy = new SimpleParameterList();
        newCopy.paramTypes.addAll(this.paramTypes);
        newCopy.paramValues.addAll(this.paramValues);
        return newCopy;
    }

    @Override
    public void clear() {
        this.paramValues.clear();
        this.paramTypes.clear();
    }

    @Override
    public List<Object> getValues() {
        return this.paramValues;
    }

    @Override
    public List<ColumnType> getTypes() {
        return this.paramTypes;
    }
}
