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
package io.arenadata.dtm.query.execution.plugin.adg.base.service.client;

import io.vertx.core.Future;

import java.util.List;

/**
 * Tarantool client
 */
public interface AdgClient {
    void close();

    Future<List<Object>> eval(String expression, Object... args);

    Future<List<Object>> call(String function, Object... args);

    Future<List<Object>> callQuery(String sql, Object... params);

    Future<List<Object>> callLoadLines(String table, Object... rows);

    boolean isAlive();
}
