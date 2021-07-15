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
package io.arenadata.dtm.query.execution.core.base.dto.cache;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

@Data
@AllArgsConstructor
public class MaterializedViewCacheValue {
    private final Entity entity;
    private UUID uuid;
    private long failsCount;
    private MaterializedViewSyncStatus status;

    public MaterializedViewCacheValue(Entity entity) {
        this.entity = entity;
        this.uuid = UUID.randomUUID();
        this.status = MaterializedViewSyncStatus.READY;
        this.failsCount = 0;
    }

    public void incrementFailsCount() {
        failsCount++;
    }

    public void markForDeletion() {
        this.uuid = null;
    }

    public boolean isMarkedForDeletion() {
        return uuid == null;
    }
}
