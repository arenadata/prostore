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
package io.arenadata.dtm.query.execution.core.base.repository.zookeeper;

import io.arenadata.dtm.query.execution.core.base.dto.metadata.DatamartInfo;
import io.vertx.core.Future;

import java.util.List;

public interface DatamartDao extends ZookeeperDao<String> {
    Future<Void> createDatamart(String name);

    Future<List<DatamartInfo>> getDatamartMeta();

    Future<List<String>> getDatamarts();

    Future<byte[]> getDatamart(String name);

    Future<Boolean> existsDatamart(String name);

    Future<Void> deleteDatamart(String name);
}
