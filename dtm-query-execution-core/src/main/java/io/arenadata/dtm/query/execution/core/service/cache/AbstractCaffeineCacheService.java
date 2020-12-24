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
package io.arenadata.dtm.query.execution.core.service.cache;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.val;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class AbstractCaffeineCacheService<K, V> extends AbstractCacheService<K, V> {
    public AbstractCaffeineCacheService(String cacheConfiguration, CacheManager cacheManager) {
        super(cacheConfiguration, cacheManager);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeIf(Predicate<K> removeCondition) {
        CaffeineCache caffeineCache = (CaffeineCache) cacheManager.getCache(cacheConfiguration);
        Cache<Object, Object> nativeCache = Objects.requireNonNull(caffeineCache).getNativeCache();
        val byRemove = nativeCache.asMap().keySet().stream()
            .map(key -> (K) key)
            .filter(removeCondition)
            .collect(Collectors.toSet());
        byRemove.forEach(this::remove);
    }
}
