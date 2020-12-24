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

import io.arenadata.dtm.query.execution.core.configuration.cache.CacheConfiguration;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.arenadata.dtm.query.execution.core.service.cache.AbstractCaffeineCacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

@Component("okDeltaCacheService")
public class OkDeltaCacheService extends AbstractCaffeineCacheService<String, OkDelta> {

    @Autowired
    public OkDeltaCacheService(@Qualifier("caffeineCacheManager") CacheManager cacheManager) {
        super(CacheConfiguration.OK_DELTA_CACHE, cacheManager);
    }

}
