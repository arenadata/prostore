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

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class DtmParameterMetadata implements ParameterMetaData {

    private final BaseConnection connection;
    private final ColumnType[] paramTypes;

    public DtmParameterMetadata(BaseConnection connection, ColumnType[] paramTypes) {
        this.connection = connection;
        this.paramTypes = paramTypes;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return paramTypes.length;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        this.checkParamIndex(param);
        return 2;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().isSigned(paramTypes[param - 1]);
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        this.checkParamIndex(param);
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        this.checkParamIndex(param);
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().getSqlType(paramTypes[param - 1]);
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().getAlias(paramTypes[param - 1]);
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().getJavaClass(paramTypes[param - 1]);
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        this.checkParamIndex(param);
        return 1;
    }

    private void checkParamIndex(int param) throws SQLException {
        if (param < 1 || param > this.paramTypes.length) {
            throw new SQLException(String.format("The parameter index is out of range: %d, number of parameters",
                this.paramTypes.length));
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(this.getClass())) {
            return iface.cast(this);
        } else {
            throw new SQLException("Cannot unwrap to " + iface.getName());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }
}
