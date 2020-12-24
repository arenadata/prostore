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

import io.arenadata.dtm.jdbc.core.BaseStatement;
import io.arenadata.dtm.jdbc.core.ConnectionFactory;
import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.QueryExecutor;
import io.arenadata.dtm.jdbc.model.ColumnInfo;
import io.arenadata.dtm.jdbc.util.DtmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class DtmConnectionImpl implements BaseConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger("io.arenadata.dtm.driver.jdbc.DtmDriver");
    public static final ZoneId DEFAULT_TIME_ZONE = ZoneId.of("UTC");
    /**
     * Hold level of resultSet
     */
    private int rsHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
    /**
     * Autocommit permission state on connection
     */
    private boolean autoCommit = true;
    private List<ColumnInfo> cachedFieldMetadata = new ArrayList<>();   //TODO need to update after changing table metadata
    /**
     * Executor for query
     */
    private QueryExecutor queryExecutor;

    public DtmConnectionImpl(String dbHost, String user, String schema, Properties info, String url) throws SQLException {
        this.queryExecutor = ConnectionFactory.openConnection(dbHost, user, schema, url, info);
        LOGGER.info("Connection created host = {} schema = {} user = {}", dbHost, schema, user);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new DtmDatabaseMetaData(this);
    }

    @Override
    public BaseStatement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public BaseStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (this.autoCommit == autoCommit) {
            return;
        }
        this.autoCommit = autoCommit;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        this.getQueryExecutor().close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.getQueryExecutor().isClosed();
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new DtmException("The connection was closed");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return this.getQueryExecutor().getDatabase();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.getQueryExecutor().setDatabase(catalog);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return rsHoldability;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BaseStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return new DtmStatement(this, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new DtmPreparedStatement(this, resultSetType, resultSetConcurrency, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new DtmPreparedStatement(this, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public String getSchema() throws SQLException {
        return "";
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public String getUrl() {
        return this.queryExecutor.getUrl();
    }

    @Override
    public String getUserName() {
        return this.queryExecutor.getUser();
    }

    @Override
    public String getDBVersionNumber() {
        return this.queryExecutor.getServerVersion();
    }

    @Override
    public List<ColumnInfo> getCachedFieldMetadata() {
        return this.cachedFieldMetadata;
    }

    @Override
    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }
}
