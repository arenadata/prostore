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
package io.arenadata.dtm.query.execution.core.service.cache.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.configuration.cache.CacheConfiguration;
import io.arenadata.dtm.query.execution.core.service.cache.AbstractCaffeineCacheService;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.cache.key.EntityKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

@Component("entityCacheService")
public class EntityCacheServiceImpl extends AbstractCaffeineCacheService<EntityKey, Entity> implements EntityCacheService {

    @Autowired
    public EntityCacheServiceImpl(@Qualifier("caffeineCacheManager") CacheManager cacheManager) {
        super(CacheConfiguration.ENTITY_CACHE, cacheManager);
    }

}
