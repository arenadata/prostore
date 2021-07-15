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
package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.BaseStatement;
import io.arenadata.dtm.jdbc.core.Field;
import io.arenadata.dtm.jdbc.core.Tuple;
import io.arenadata.dtm.jdbc.util.DtmSqlException;
import lombok.SneakyThrows;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class DtmResultSet extends AbstractResultSet {
    private final Field[] fields;
    private final BaseConnection connection;
    private final BaseStatement statement;
    private final ZoneId zoneId;
    protected List<Tuple> rows;
    protected SQLWarning warnings = null;
    /**
     * True if the last obtained column value was SQL NULL
     */
    protected boolean wasNullFlag = false;
    protected int fetchSize = 0;
    private int currentRow = -1;
    private Tuple thisRow;
    private ResultSetMetaData rsMetaData;
    private Map<String, Integer> columnNameIndexMap;

    public DtmResultSet(BaseConnection connection, BaseStatement statement, Field[] fields, List<Tuple> tuples, ZoneId timeZone) {
        this.connection = connection;
        this.statement = statement;
        this.fields = fields;
        this.rows = tuples;
        this.thisRow = (tuples == null || tuples.isEmpty()) ?
                new Tuple(0) : tuples.get(0);
        this.zoneId = timeZone;
    }

    public static DtmResultSet createEmptyResultSet() {
        return new DtmResultSet(null,
                null,
                new Field[]{new Field("", ColumnType.VARCHAR)},
                Collections.emptyList(),
                DtmConnectionImpl.DEFAULT_TIME_ZONE);
    }

    @Override
    public boolean next() {
        if (this.currentRow + 1 >= this.rows.size()) {
            return false;
        } else {
            this.currentRow++;
        }
        initRowBuffer();
        return true;
    }

    @Override
    public boolean first() throws SQLException {
        if (this.rows.isEmpty()) {
            return false;
        }

        this.currentRow = 0;
        initRowBuffer();

        return true;
    }

    private void initRowBuffer() {
        this.thisRow = this.rows.get(this.currentRow);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = this.getValue(columnIndex);
        return value == null ? null : value.toString();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return this.getString(findColumn(columnLabel));
    }


    @SneakyThrows
    @Override
    public int findColumn(String columnLabel) {
        int col = findColumnIndex(columnLabel);
        if (col == 0) {
            throw new DtmSqlException("Column not found: " + columnLabel);
        }
        return col;
    }

    private int findColumnIndex(String columnName) {
        if (this.columnNameIndexMap == null) {
            this.columnNameIndexMap = createColumnNameIndexMap(this.fields);
        }
        Integer index = this.columnNameIndexMap.get(columnName);
        if (index != null) {
            return index;
        } else {
            return 0;
        }
    }

    private Map<String, Integer> createColumnNameIndexMap(Field[] fields) {
        Map<String, Integer> indexMap = new HashMap<>(fields.length * 2);

        for (int i = fields.length - 1; i >= 0; --i) {
            String columnLabel = fields[i].getColumnLabel();
            indexMap.put(columnLabel, i + 1);
        }

        return indexMap;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        if (this.rsMetaData == null) {
            this.rsMetaData = createMetaData();
        }
        return this.rsMetaData;
    }

    protected ResultSetMetaData createMetaData() {
        return new DtmResultSetMetaData(this.connection, this.fields);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        final Field field = this.fields[columnIndex - 1];
        if (this.getValue(columnIndex) == null) {
            return null;
        } else {
            if (field == null) {
                this.wasNullFlag = true;
                return null;
            } else {
                switch (field.getDtmType()) {
                    case INT:
                    case BIGINT:
                    case INT32:
                        return this.getLong(columnIndex);
                    case VARCHAR:
                    case ANY:
                    case CHAR:
                    case UUID:
                    case BLOB:
                    case LINK:
                        return this.getString(columnIndex);
                    case FLOAT:
                        return this.getFloat(columnIndex);
                    case DOUBLE:
                        return this.getDouble(columnIndex);
                    case BOOLEAN:
                        return this.getBoolean(columnIndex);
                    case DATE:
                        return this.getDate(columnIndex);
                    case TIME:
                        return this.getTime(columnIndex);
                    case TIMESTAMP:
                        return this.getTimestamp(columnIndex);
                    default:
                        throw new SQLException(String.format("Column type %s for index %s not found!",
                                field.getDtmType(), columnIndex));
                }
            }
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return this.getObject(this.findColumn(columnLabel));
    }

    private Object getValue(int columnIndex) throws SQLException {
        if (this.thisRow == null) {
            throw new DtmSqlException("ResultSet not positioned properly, perhaps you need to call next.");
        } else {
            return this.thisRow.get(columnIndex - 1);
        }
    }

    @Override
    public void close() throws SQLException {
        rows = null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value != null && (boolean) value;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? 0 : Byte.parseByte(value.toString());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? 0 : (Short) value;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? 0 : (Integer) value;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        //FIXME Dbeaver used this method for received value of INT field
        final Object value = this.getValue(columnIndex);
        if (value == null) {
            return 0L;
        } else {
            return Long.parseLong(value.toString());
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? 0 : ((Number) value).floatValue();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = this.getValue(columnIndex);
        if (value == null) {
            return 0.0D;
        } else {
            return ((Number) value).doubleValue();
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        String string = this.getString(columnIndex);
        if (string == null) {
            return null;
        } else {
            BigDecimal result = new BigDecimal(string);
            return result.setScale(scale, RoundingMode.HALF_UP);
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? new byte[0] : value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        if (value != null) {
            return Date.valueOf(LocalDate.ofEpochDay(((Number) value).longValue()));
        } else {
            return null;
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object value = this.getValue(columnIndex);
        if (value != null) {
            long longValue = ((Number) value).longValue();
            long epochSeconds = longValue / 1000000;
            return new Time(Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds,
                    getNanos(columnIndex, longValue)
            ), zoneId)).getTime());
        } else {
            return null;
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        if (value == null) {
            return null;
        } else {
            Number numberValue = (Number) value;
            int nanos = getNanos(columnIndex, numberValue);
            long epochSeconds = numberValue.longValue() / 1000000;
            return Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanos), zoneId));
        }
    }

    private int getNanos(int columnIndex, Number tsValue) {
        Field field = fields[columnIndex - 1];
        if (field.getSize() != null) {
            int q = (int) Math.pow(10, 6 - field.getSize());
            return (int) (tsValue.longValue() % 1000000 / q * 1000 * q);
        } else {
            return 0;
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return this.getBoolean(this.findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return this.getByte(this.findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return this.getShort(this.findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return this.getInt(this.findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return this.getLong(this.findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return this.getFloat(this.findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return this.getDouble(this.findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return this.getBigDecimal(this.findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return this.getBytes(this.findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return this.getDate(this.findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return this.getDate(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return this.getTime(this.findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return this.getTimestamp(this.findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String value = this.getString(columnIndex);
        return value == null ? null : new BigDecimal(value);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return this.getBigDecimal(this.findColumn(columnLabel));
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        Object value = this.getValue(columnIndex);
        if (value != null) {
            return Date.valueOf((LocalDate) value);
        } else {
            return null;
        }
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        Object value = this.getValue(columnIndex);
        if (value != null) {
            long longValue = ((Number) value).longValue();
            long epochSeconds = longValue / 1000000;
            return new Time(Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds,
                    getNanos(columnIndex, longValue)
            ), cal.getTimeZone().toZoneId())).getTime());
        } else {
            return null;
        }
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return this.getTime(this.findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        final Object value = this.getValue(columnIndex);
        if (value == null) {
            return null;
        } else {
            Number numberValue = (Number) value;
            int nanos = getNanos(columnIndex, numberValue);
            long epochSeconds = numberValue.longValue() / 1000000;
            return Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanos), cal.getTimeZone().toZoneId()));
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return this.getTimestamp(this.findColumn(columnLabel), cal);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNullFlag;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        warnings = null;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows >= 0) {
            fetchSize = rows;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return (rows == null);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) {
            return null;
        }
        String stringValue = getString(columnIndex);
        return new ByteArrayInputStream(stringValue.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) {
            return null;
        }
        byte[] b = getBytes(columnIndex);
        if (b != null) {
            return new ByteArrayInputStream(b);
        }
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumnIndex(columnLabel));
    }

    public int getRowsSize() {
        return rows.size();
    }
}
