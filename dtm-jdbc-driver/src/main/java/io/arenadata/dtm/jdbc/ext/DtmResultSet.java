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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
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
        if (currentRow + 1 >= rows.size()) {
            return false;
        } else {
            currentRow++;
        }
        initRowBuffer();
        return true;
    }

    @Override
    public boolean first() throws SQLException {
        if (rows.isEmpty()) {
            return false;
        }

        currentRow = 0;
        initRowBuffer();

        return true;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getRawValue(columnIndex);
        return value == null ? null : value.toString();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
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

    @Override
    public ResultSetMetaData getMetaData() {
        if (rsMetaData == null) {
            rsMetaData = createMetaData();
        }
        return rsMetaData;
    }

    protected ResultSetMetaData createMetaData() {
        return new DtmResultSetMetaData(connection, fields);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        final Field field = fields[columnIndex - 1];
        if (getRawValue(columnIndex) == null) {
            return null;
        } else {
            if (field == null) {
                wasNullFlag = true;
                return null;
            } else {
                switch (field.getDtmType()) {
                    case INT:
                    case BIGINT:
                    case INT32:
                        return getLong(columnIndex);
                    case VARCHAR:
                    case ANY:
                    case CHAR:
                    case UUID:
                    case BLOB:
                    case LINK:
                        return getString(columnIndex);
                    case FLOAT:
                        return getFloat(columnIndex);
                    case DOUBLE:
                        return getDouble(columnIndex);
                    case BOOLEAN:
                        return getBoolean(columnIndex);
                    case DATE:
                        return getDate(columnIndex);
                    case TIME:
                        return getTime(columnIndex);
                    case TIMESTAMP:
                        return getTimestamp(columnIndex);
                    default:
                        throw new SQLException(String.format("Column type %s for index %s not found!",
                                field.getDtmType(), columnIndex));
                }
            }
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public void close() throws SQLException {
        rows = null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        return value != null && (boolean) value;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        return value == null ? 0 : Byte.parseByte(value.toString());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        return value == null ? 0 : (Short) value;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        return value == null ? 0 : (Integer) value;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        //FIXME Dbeaver used this method for received value of INT field
        final Object value = getRawValue(columnIndex);
        if (value == null) {
            return 0L;
        } else {
            return Long.parseLong(value.toString());
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        return value == null ? 0 : ((Number) value).floatValue();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getRawValue(columnIndex);
        if (value == null) {
            return 0.0D;
        } else {
            return ((Number) value).doubleValue();
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        String string = getString(columnIndex);
        if (string == null) {
            return null;
        } else {
            BigDecimal result = new BigDecimal(string);
            return result.setScale(scale, RoundingMode.HALF_UP);
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        return value == null ? new byte[0] : value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, Calendar.getInstance());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, Calendar.getInstance());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, Calendar.getInstance());
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        return value == null ? null : new BigDecimal(value);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        Object value = getRawValue(columnIndex);
        if (value == null) {
            return null;
        }

        LocalDate localDate = LocalDate.ofEpochDay(((Number) value).longValue());
        cal.set(localDate.getYear(), localDate.getMonthValue() - 1, localDate.getDayOfMonth(),
                0, 0, 0);
        return new Date(cal.getTimeInMillis());
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        Object value = getRawValue(columnIndex);
        if (value == null) {
            return null;
        }
        Instant instant = convertToCalendarInstant(columnIndex, cal, (Number) value);
        return new Time(instant.toEpochMilli());
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        final Object value = getRawValue(columnIndex);
        if (value == null) {
            return null;
        }

        Instant instant = convertToCalendarInstant(columnIndex, cal, (Number) value);
        Timestamp timestamp = new Timestamp(instant.toEpochMilli());
        timestamp.setNanos(instant.getNano());
        return timestamp;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
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
        Object value = getRawValue(columnIndex);
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
        Object value = getRawValue(columnIndex);
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

    private void initRowBuffer() {
        thisRow = rows.get(currentRow);
    }

    private int findColumnIndex(String columnName) {
        if (columnNameIndexMap == null) {
            columnNameIndexMap = createColumnNameIndexMap(fields);
        }
        Integer index = columnNameIndexMap.get(columnName);
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

    private Object getRawValue(int columnIndex) throws SQLException {
        if (thisRow == null) {
            throw new DtmSqlException("ResultSet not positioned properly, perhaps you need to call next.");
        } else {
            return thisRow.get(columnIndex - 1);
        }
    }

    private Instant convertToCalendarInstant(int columnIndex, Calendar cal, Number value) {
        long timeValue = value.longValue();
        long epochSeconds = timeValue / 1000000L;
        Instant backendInstant = Instant.ofEpochSecond(epochSeconds, getNanos(columnIndex, timeValue));
        return LocalDateTime.ofInstant(backendInstant, zoneId)
                .atZone(cal.getTimeZone().toZoneId())
                .toInstant();
    }

    private int getNanos(int columnIndex, long tsValue) {
        Field field = fields[columnIndex - 1];
        if (field.getSize() != null) {
            int q = (int) Math.pow(10, 6 - field.getSize());
            return (int) (tsValue % 1000000 / q * 1000 * q);
        } else {
            return 0;
        }
    }
}
