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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtResultTranslator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;

public class TtClientImpl implements TtClient {

  private TarantoolDatabaseProperties tarantoolProperties;
  private TtResultTranslator resultTranslator;
  private TarantoolClient client;

  public TtClientImpl(TarantoolDatabaseProperties tarantoolProperties, TtResultTranslator resultTranslator) {
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
        throw new IllegalStateException(e);
      }
    };
    this.client = new TarantoolClientImpl(socketChannelProvider, config);
  }

  @Override
  public void close() {
    this.client.close();
  }

  @Override
  public void eval(Handler<AsyncResult<List<?>>> handler, String expression, Object... args) {
    client.composableAsyncOps().eval(expression, args)
      .thenAccept(res -> {
        handler.handle(Future.succeededFuture(resultTranslator.translate(res)));
      })
      .exceptionally(e -> {
        handler.handle(Future.failedFuture(e));
        return null;
      });
  }

  @Override
  public void call(Handler<AsyncResult<List<?>>> handler, String function, Object... args) {
    client.composableAsyncOps().call(function, args)
      .thenAccept(res -> {
        handler.handle(Future.succeededFuture(resultTranslator.translate(res)));
      })
      .exceptionally(e -> {
        handler.handle(Future.failedFuture(e));
        return null;
      });
  }

  @Override
  public void callQuery(Handler<AsyncResult<List<?>>> handler, String sql, Object... params) {
    if (params == null || params.length == 0) {
      call(handler, "query", sql);
    } else {
      call(handler, "query", sql, params);
    }
  }

  @Override
  public void callLoadLines(Handler<AsyncResult<List<?>>> handler, String table, Object... rows) {
    call(handler, "load_lines", table, rows);
  }

  @Override
  public boolean isAlive() {
    return client.isAlive();
  }
}
