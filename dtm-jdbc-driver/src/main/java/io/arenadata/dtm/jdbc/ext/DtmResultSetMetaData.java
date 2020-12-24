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
package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.Field;
import io.arenadata.dtm.jdbc.model.ColumnInfo;
import io.arenadata.dtm.jdbc.util.DtmException;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import lombok.SneakyThrows;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class DtmResultSetMetaData implements ResultSetMetaData {
    protected final BaseConnection connection;
    protected final List<ColumnMetadata> columnMetadata;

    public DtmResultSetMetaData(BaseConnection connection, List<ColumnMetadata> columnMetadata) {
        this.connection = connection;
        this.columnMetadata = columnMetadata;
    }

    @Override
    public int getColumnCount() {
        return this.columnMetadata.size();
    }

    @SneakyThrows
    @Override
    public String getColumnLabel(int column) {
        return this.columnMetadata.get(column - 1).getName();
    }

    @Override
    public String getColumnName(int column) {
        return this.getColumnLabel(column);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return this.getFieldMetadata(column).getDatamartMnemonic();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return this.getFieldMetadata(column).getNullable() ? 1 : 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 80;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return this.getFieldMetadata(column).getLength();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return this.getFieldMetadata(column).getAccuracy();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return this.getFieldMetadata(column).getEntityMnemonic();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return this.getFieldMetadata(column).getDatamartMnemonic();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        ColumnType type = this.columnMetadata.get(column - 1).getType();
        if (type == ColumnType.INT) {
            return JDBCType.INTEGER.getVendorTypeNumber();
        }
        if (type == ColumnType.ANY) {
            return JDBCType.VARCHAR.getVendorTypeNumber();
        } else {
            return JDBCType
                    .valueOf(type.name())
                    .getVendorTypeNumber();
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return this.columnMetadata.get(column - 1).getType().name();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return "java.lang.String";
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    private ColumnInfo getFieldMetadata(int column) throws SQLException {
        final List<ColumnInfo> cachedFieldMetadata = this.connection.getCachedFieldMetadata();
        if (cachedFieldMetadata.isEmpty()) {
            throw new SQLException("Field metadata list is empty");
        }
        return cachedFieldMetadata.get(column);
    }
}
