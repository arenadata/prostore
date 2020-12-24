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
package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Клиент Tarantool
 */
public interface TtClient {
  void close();
  void eval(Handler<AsyncResult<List<?>>> handler, String expression, Object... args);
  void call(Handler<AsyncResult<List<?>>> handler, String function, Object... args);
  void callQuery(Handler<AsyncResult<List<?>>> handler, String sql, Object... params);
  void callLoadLines(Handler<AsyncResult<List<?>>> handler, String table, Object... rows);
  boolean isAlive();
}
