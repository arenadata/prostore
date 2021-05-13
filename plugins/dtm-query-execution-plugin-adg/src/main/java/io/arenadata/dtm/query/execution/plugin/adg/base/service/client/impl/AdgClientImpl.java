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
package io.arenadata.dtm.query.execution.plugin.adg.base.service.client.impl;

import io.arenadata.dtm.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgClient;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgResultTranslator;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;

public class AdgClientImpl implements AdgClient {

    private final TarantoolDatabaseProperties tarantoolProperties;
    private final AdgResultTranslator resultTranslator;
    private TarantoolClient client;

    public AdgClientImpl(TarantoolDatabaseProperties tarantoolProperties, AdgResultTranslator resultTranslator) {
        this.tarantoolProperties = tarantoolProperties;
        this.resultTranslator = resultTranslator;
        init();
    }

    private void init() {
        TarantoolClientConfig config = new TarantoolClientConfig();
        config.username = tarantoolProperties.getUser();
        config.password = tarantoolProperties.getPassword();
        config.operationExpiryTimeMillis = tarantoolProperties.getOperationTimeout();
        config.retryCount = tarantoolProperties.getRetryCount();
        config.initTimeoutMillis = tarantoolProperties.getInitTimeoutMillis();
        SocketChannelProvider socketChannelProvider = (i, throwable) -> {
            SocketChannel channel;
            try {
                channel = SocketChannel.open();
                channel.socket().connect(new InetSocketAddress(tarantoolProperties.getHost(), tarantoolProperties.getPort()));
                return channel;
            } catch (IOException e) {
                throw new DataSourceException("Error in socket provider", e);
            }
        };
        this.client = new TarantoolClientImpl(socketChannelProvider, config);
    }

    @Override
    public void close() {
        this.client.close();
    }

    @Override
    public Future<List<Object>> eval(String expression, Object... args) {
        return Future.future(promise -> {
            client.composableAsyncOps().eval(expression, args)
                    .thenAccept(res -> {
                        promise.complete(resultTranslator.translate((List<Object>) res));
                    })
                    .exceptionally(e -> {
                        promise.fail(new DataSourceException(e));
                        return null;
                    });
        });
    }

    @Override
    public Future<List<Object>> call(String function, Object... args) {
        return Future.future(promise -> {
            client.composableAsyncOps().call(function, args)
                    .thenAccept(res -> {
                        promise.complete(resultTranslator.translate((List<Object>) res));
                    })
                    .exceptionally(e -> {
                        promise.fail(new DataSourceException(e));
                        return null;
                    });
        });
    }

    @Override
    public Future<List<Object>> callQuery(String sql, Object... params) {
        if (params == null || params.length == 0) {
            return call("query", sql);
        } else {
            return call("query", sql, params);
        }
    }

    @Override
    public Future<List<Object>> callLoadLines(String table, Object... rows) {
        return call("load_lines", table, rows);
    }

    @Override
    public boolean isAlive() {
        return client.isAlive();
    }
}
