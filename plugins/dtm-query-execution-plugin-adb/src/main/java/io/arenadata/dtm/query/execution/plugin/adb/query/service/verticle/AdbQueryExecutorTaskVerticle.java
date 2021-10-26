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
package io.arenadata.dtm.query.execution.plugin.adb.query.service.verticle;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adb.base.configuration.properties.AdbProperties;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.pool.AdbConnectionFactory;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.pool.AdbConnectionPool;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.Message;
import lombok.val;

import java.util.Map;

public class AdbQueryExecutorTaskVerticle extends AbstractVerticle {
    private final AdbProperties adbProperties;
    private final SqlTypeConverter typeConverter;
    private final SqlTypeConverter sqlTypeConverter;
    private final Map<String, AdbExecutorTask> taskMap;
    private final Map<String, AsyncResult<?>> resultMap;
    private final AdbConnectionFactory connectionFactory;
    private AdbQueryExecutor adbQueryExecutor;

    public AdbQueryExecutorTaskVerticle(AdbProperties adbProperties,
                                        SqlTypeConverter typeConverter,
                                        SqlTypeConverter sqlTypeConverter,
                                        Map<String, AdbExecutorTask> taskMap,
                                        Map<String, AsyncResult<?>> resultMap,
                                        AdbConnectionFactory connectionFactory) {
        this.adbProperties = adbProperties;
        this.typeConverter = typeConverter;
        this.sqlTypeConverter = sqlTypeConverter;
        this.taskMap = taskMap;
        this.resultMap = resultMap;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void start() throws Exception {
        val pool = new AdbConnectionPool(connectionFactory, vertx, adbProperties.getPoolSize());
        adbQueryExecutor = new AdbQueryExecutor(pool, adbProperties.getFetchSize(), typeConverter, sqlTypeConverter);

        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE.getTopic(), this::executeHandler);
        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE_WITH_CURSOR.getTopic(), this::executeWithCursorHandler);
        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE_WITH_PARAMS.getTopic(), this::executeWithParamsHandler);
        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE_UPDATE.getTopic(), this::executeUpdateHandler);
        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE_IN_TRANSACTION.getTopic(), this::executeInTransactionHandler);
    }

    private void executeHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.execute(adbExecutorTask.getSql(), adbExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });
    }

    private void executeWithCursorHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.executeWithCursor(adbExecutorTask.getSql(), adbExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeWithParamsHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.executeWithParams(adbExecutorTask.getSql(), adbExecutorTask.getParams(), adbExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeUpdateHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.executeUpdate(adbExecutorTask.getSql())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeInTransactionHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.executeInTransaction(adbExecutorTask.getPreparedStatementRequests())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }
}
