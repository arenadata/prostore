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

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.base.dto.metadata.DatamartEntity;
import io.vertx.core.Future;

import java.util.List;

public interface EntityDao extends ZookeeperDao<Entity> {
    Future<List<DatamartEntity>> getEntitiesMeta(String datamartMnemonic);

    Future<Void> createEntity(Entity entity);

    Future<Void> updateEntity(Entity entity);

    Future<Boolean> existsEntity(String datamartMnemonic, String entityName);

    Future<Void> deleteEntity(String datamartMnemonic, String entityName);

    Future<Entity> getEntity(String datamartMnemonic, String entityName);

    Future<List<String>> getEntityNamesByDatamart(String datamartMnemonic);
}
