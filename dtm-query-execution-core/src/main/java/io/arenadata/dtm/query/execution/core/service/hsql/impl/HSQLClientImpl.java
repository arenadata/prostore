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
package io.arenadata.dtm.query.execution.core.service.hsql.impl;

import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.function.*;

@Slf4j
public class HSQLClientImpl implements HSQLClient {

    private static final String JDBC_DRIVER = "org.hsqldb.jdbc.JDBCDriver";
    private static final String INMEMORY_DATABASE_URL = "jdbc:hsqldb:mem:cachedb";

    private JDBCClient jdbcClient;

    public HSQLClientImpl(Vertx vertx) {
        this.jdbcClient = JDBCClient.create(vertx, new JsonObject()
                .put("url", INMEMORY_DATABASE_URL)
                .put("driver_class", JDBC_DRIVER)
                .put("max_pool_size", 30)
                .put("user", "SA")
                .put("password", "")
                .put("acquire_retry_attempts", 3)
                .put("break_after_acquire_failure", true));

    }

    @Override
    public Future<Void> executeQuery(String query){
        return execute(String.format("Error occurred while executing query: %s", query),
            (sqlConnection, handler) -> sqlConnection.execute(query, handler));
    }

    @Override
    public Future<Void> executeBatch(List<String> queries) {
        return execute(String.format("Error while executing queries batch:\n %s", String.join(";\n", queries)),
            (sqlConnection, handler) -> sqlConnection.batch(queries,
                batchHandler -> handler.handle(batchHandler.succeeded() ? Future.succeededFuture()
                        : Future.failedFuture(batchHandler.cause()))));
    }

    @Override
    public Future<ResultSet> getQueryResult(String query){
        return execute(String.format("Error occurred while executing query: %s", query),
            (sqlConnection, handler) -> sqlConnection.query(query, handler));
    }

    private <T> Future<T> execute(String error, BiConsumer<SQLConnection, Handler<AsyncResult<T>>> consumer) {
        return Future.future(promise -> jdbcClient.getConnection(conn -> {
            if (conn.succeeded()) {
                val connection = conn.result();
                consumer.accept(connection, handler -> {
                    connection.close();
                    if (handler.succeeded()) {
                        promise.complete(handler.result());
                    } else {
                        log.error(error, handler.cause());
                        promise.fail(handler.cause());
                    }
                });
            } else {
                log.error("Could not open hsqldb connection", conn.cause());
                promise.fail(conn.cause());
            }
        }));
    }
}
