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
package io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper;

import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartInfo;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;

public interface DatamartDao extends ZookeeperDao<String> {
    Future<Void> createDatamart(String name);

    void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler);

    Future<List<String>> getDatamarts();

    Future<?> getDatamart(String name);

    Future<Boolean> existsDatamart(String name);

    Future<Void> deleteDatamart(String name);
}
