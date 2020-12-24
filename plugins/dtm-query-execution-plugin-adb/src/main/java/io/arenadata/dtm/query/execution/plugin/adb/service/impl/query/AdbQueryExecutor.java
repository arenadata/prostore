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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.query;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.reactiverse.pgclient.*;
import io.reactiverse.pgclient.impl.ArrayTuple;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Slf4j
public class AdbQueryExecutor implements DatabaseExecutor {

    private final PgPool pool;
    private final int fetchSize;
    private final SqlTypeConverter typeConverter;

    public AdbQueryExecutor(PgPool pool, int fetchSize, SqlTypeConverter typeConverter) {
        this.pool = pool;
        this.fetchSize = fetchSize;
        this.typeConverter = typeConverter;
    }

    @Override
    public void execute(String sql, List<ColumnMetadata> metadata, Handler<AsyncResult<List<Map<String, Object>>>> resultHandler) {
        pool.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                log.debug("Execute query: [{}]", sql);
                PgConnection conn = ar1.result();
                conn.prepare(sql, ar2 -> {
                    if (ar2.succeeded()) {
                        PgCursor cursor = ar2.result().cursor();
                        do {
                            cursor.read(fetchSize, res -> {
                                if (res.succeeded()) {
                                    try {
                                        List<Map<String, Object>> result = createResult(metadata, res.result());
                                        resultHandler.handle(Future.succeededFuture(result));
                                    } catch (Exception e) {
                                        tryCloseConnect(conn);
                                        log.error("Error converting ADB values to jdbc types!", e);
                                        resultHandler.handle(Future.failedFuture(e));
                                    }
                                } else {
                                    tryCloseConnect(conn);
                                    log.error("Error fetching cursor", res.cause());
                                    resultHandler.handle(Future.failedFuture(res.cause()));
                                }
                            });
                        } while (cursor.hasMore());
                        tryCloseConnect(conn);
                    } else {
                        tryCloseConnect(conn);
                        log.error("Request preparation error!", ar2.cause());
                        resultHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                log.error("Connection error!", ar1.cause());
                resultHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    private void tryCloseConnect(PgConnection conn) {
        try {
            conn.close();
        } catch (Exception e) {
            log.warn("Error closing connection: {}", e.getMessage());
        }
    }

    private List<Map<String, Object>> createResult(List<ColumnMetadata> metadata,
                                                   io.reactiverse.pgclient.PgRowSet pgRowSet) {
        List<Map<String, Object>> result = new ArrayList<>();
        Function<Row, Map<String, Object>> func = metadata.isEmpty()
                ? row -> createRowMap(row, pgRowSet.columnsNames().size())
                : row -> createRowMap(metadata, row);
        for (io.reactiverse.pgclient.Row row : pgRowSet) {
            result.add(func.apply(row));
        }
        return result;
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata, io.reactiverse.pgclient.Row row) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < metadata.size(); i++) {
            ColumnMetadata columnMetadata = metadata.get(i);
            rowMap.put(columnMetadata.getName(),
                typeConverter.convert(columnMetadata.getType(), row.getValue(i)));
        }
        return rowMap;
    }

    private Map<String, Object> createRowMap(io.reactiverse.pgclient.Row row, int size) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            rowMap.put(row.getColumnName(i), row.getValue(i));
        }
        return rowMap;
    }

    @Override
    public void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler) {
        log.debug("ADB. execute update: [{}]", sql);
        pool.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                PgConnection conn = ar1.result();
                conn.query(sql, ar2 -> {
                    if (ar2.succeeded()) {
                        log.debug("ADB. update completed: [{}]", sql);
                        completionHandler.handle(Future.succeededFuture());
                    } else {
                        log.error("ADB. update error: [{}]: {}", sql, ar2.cause().getMessage());
                        completionHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                    tryCloseConnect(conn);
                });
            } else {
                log.error("Connection error!", ar1.cause());
                completionHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    @Override
    public void executeWithParams(String sql, List<Object> params, List<ColumnMetadata> metadata, Handler<AsyncResult<?>> resultHandler) {
        pool.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                PgConnection conn = ar1.result();
                conn.preparedQuery(sql, new ArrayTuple(params), ar2 -> {
                    if (ar2.succeeded()) {
                        try {
                            List<Map<String, Object>> result = createResult(metadata, ar2.result());
                            resultHandler.handle(Future.succeededFuture(result));
                        } catch (Exception e) {
                            tryCloseConnect(conn);
                            log.error("Error converting ADB values to jdbc types!", e);
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        resultHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                log.error("Connection error!", ar1.cause());
                resultHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    @Override
    public void executeInTransaction(List<PreparedStatementRequest> requests, Handler<AsyncResult<Void>> handler) {
        beginTransaction(pool)
            .compose(tx -> Future.future((Promise<PgTransaction> promise) -> {
                Future<PgTransaction> lastFuture = null;
                for (PreparedStatementRequest st : requests) {
                    log.debug("Execute query: {} with params: {}", st.getSql(), st.getParams());
                    if (lastFuture == null) {
                        lastFuture = executeTx(st, tx);
                    } else {
                        lastFuture = lastFuture.compose(s -> executeTx(st, tx));
                    }
                }
                if (lastFuture == null) {
                    handler.handle(Future.succeededFuture());
                    return;
                }
                lastFuture.onSuccess(s -> promise.complete(tx))
                    .onFailure(fail -> promise.fail(fail.toString()));
            }))
            .compose(this::commitTransaction)
            .onSuccess(s -> handler.handle(Future.succeededFuture()))
            .onFailure(f -> handler.handle(Future.failedFuture(
                String.format("Error executing queries: %s", f.getMessage()))));
    }

    private Future<PgTransaction> beginTransaction(PgPool pgPool) {
        return Future.future((Promise<PgTransaction> promise) -> pgPool.begin(ar -> {
            if (ar.succeeded()) {
                log.trace("Transaction began");
                promise.complete(ar.result());
            } else {
                log.error("Connection error", ar.cause());
                promise.fail(ar.cause());
            }
        }));
    }

    private Future<PgTransaction> executeTx(PreparedStatementRequest request, PgTransaction tx) {
        return Future.future((Promise<PgTransaction> promise) -> tx.query(request.getSql(), rs -> {
            if (rs.succeeded()) {
                promise.complete(tx);
            } else {
                log.error("Error executing query [{}]", request.getSql(), rs.cause());
                promise.fail(rs.cause());
            }
        }));
    }

    private Future<Void> commitTransaction(PgTransaction trx) {
        return Future.future((Promise<Void> promise) ->
            trx.commit(txCommit -> {
                if (txCommit.succeeded()) {
                    log.debug("Transaction succeeded");
                    promise.complete();
                } else {
                    log.error("Transaction failed [{}]", txCommit.cause().getMessage());
                    promise.fail(txCommit.cause());
                }
            }));
    }

}
