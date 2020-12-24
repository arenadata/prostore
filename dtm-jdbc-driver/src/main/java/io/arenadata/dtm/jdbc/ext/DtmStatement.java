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

import io.arenadata.dtm.jdbc.core.*;
import io.arenadata.dtm.jdbc.util.DtmException;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

@Slf4j
public class DtmStatement implements BaseStatement {
    /**
     * DTM connection
     */
    protected final BaseConnection connection;
    /**
     * ResultSet type (ResultSet.TYPE_xxx)
     */
    protected final int resultSetScrollType;
    /**
     * Is result set updatable (ResultSet.CONCUR_xxx)
     */
    protected final int concurrency;
    /**
     * Max rows count
     */
    protected long maxRows;
    /**
     * Return rows count
     */
    protected int fetchSize;
    /**
     * Result set wrapper
     */
    protected ResultSetWrapper result;
    /**
     * Is connection closed
     */
    private boolean isClosed;

    public DtmStatement(BaseConnection c, int rsType, int rsConcurrency) {
        this.connection = c;
        this.resultSetScrollType = rsType;
        this.concurrency = rsConcurrency;
        this.isClosed = false;
        this.result = null;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (executeInternal(sql, fetchSize, Statement.NO_GENERATED_KEYS)) {
            return this.getSingleResultSet();
        }
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        log.debug("execute: {}", sql);
        return executeInternal(sql, fetchSize, Statement.NO_GENERATED_KEYS);
    }

    private boolean executeInternal(String sql, int fetchSize, int noGeneratedKeys) throws SQLException {
        log.debug("executeInternal: {}", sql);
        List<Query> queries = this.connection.getQueryExecutor().createQuery(sql);
        DtmResultHandler resultHandler = new DtmResultHandler();
        if (queries.size() == 1) {
            this.connection.getQueryExecutor().execute(queries.get(0), Collections.emptyList(), resultHandler);
        } else {
            this.connection.getQueryExecutor().execute(queries, Collections.emptyList(), resultHandler);
        }
        if (resultHandler.getException() == null) {
            this.result = resultHandler.getResult();
            return result != null;
        } else {
            throw new SQLException(resultHandler.getException());
        }
    }

    protected ResultSet getSingleResultSet() throws SQLException {
        synchronized (this) {
            this.checkClosed();
            ResultSetWrapper result = this.result;
            if (result.getNext() != null) {
                throw new DtmException("Multiple ResultSets were returned by the query.");
            } else {
                return result.getResultSet();
            }
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        log.debug("executeUpdate: {}", sql);
        execute(sql);
        return 1;
    }

    @Override
    public void close() throws SQLException {
        this.isClosed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return (int) maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        this.checkClosed();
        return this.result == null ? null : this.result.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        this.checkClosed();
        if (this.result != null && this.result.getResultSet() != null) {
            this.result.getResultSet().close();
        }

        if (this.result != null) {
            this.result = this.result.getNext();
        }
        return this.result != null && this.result.getResultSet() != null;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0 && rows != Integer.MIN_VALUE) {
            throw new SQLException(String.format("Incorrect %d value for block size", rows));
        } else if (rows == Integer.MIN_VALUE) {
            //for compatibility Integer.MIN_VALUE is transform to 0 => streaming
            this.fetchSize = 1;
            return;
        }
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return resultSetScrollType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return executeInternal(sql, fetchSize, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return executeInternal(sql, fetchSize, Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return executeInternal(sql, fetchSize, Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    protected void checkClosed() throws SQLException {
        if (this.isClosed()) {
            throw new DtmException("This statement has been closed.");
        }
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        close();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public ResultSet createDriverResultSet(List<Field[]> fields, List<ColumnMetadata> metadata) {
        return createResultSet(fields, metadata, DtmConnectionImpl.DEFAULT_TIME_ZONE);
    }

    private DtmResultSet createResultSet(List<Field[]> fields, List<ColumnMetadata> metadata, ZoneId timeZone) {
        return new DtmResultSet(this.connection, fields, metadata, timeZone);
    }

    public class DtmResultHandler extends ResultHandlerBase {
        private ResultSetWrapper results;
        private ResultSetWrapper lastResult;

        ResultSetWrapper getResult() {
            return this.results;
        }

        private void append(ResultSetWrapper newResult) {
            if (this.results == null) {
                this.lastResult = this.results = newResult;
            } else {
                this.lastResult.append(newResult);
            }
        }

        @Override
        public void handleResultRows(Query query, List<Field[]> fields, List<ColumnMetadata> metadata, ZoneId timeZone) {
            try {
                ResultSet rs = createResultSet(fields, metadata, timeZone);
                this.append(new ResultSetWrapper((DtmResultSet) rs));
            } catch (Exception e) {
                this.handleError(new SQLException(e));
            }
        }
    }
}
