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
package io.arenadata.dtm.cache.service;

import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;

import java.util.List;
import java.util.function.Predicate;

public class EvictQueryTemplateCacheServiceImpl implements EvictQueryTemplateCacheService {
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService;
    private final List<CacheService<QueryTemplateKey, QueryTemplateValue>> cacheServiceList;

    public EvictQueryTemplateCacheServiceImpl(CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService,
                                              List<CacheService<QueryTemplateKey, QueryTemplateValue>> cacheServiceList) {
        this.cacheService = cacheService;
        this.cacheServiceList = cacheServiceList;
    }

    @Override
    public void evictByDatamartName(String datamartName) {
        remove(datamart -> datamart.getMnemonic().equals(datamartName));
    }

    @Override
    public void evictByEntityName(String datamartName, String entityName) {
        remove(datamart -> datamart.getMnemonic().equals(datamartName)
                && datamart.getEntities().stream()
                .anyMatch(dmEntity -> dmEntity.getName().equals(entityName)));
    }

    private void remove(Predicate<Datamart> predicate) {
        Predicate<QueryTemplateKey> templatePredicate = queryTemplateKey ->
                queryTemplateKey.getLogicalSchema().stream()
                        .anyMatch(predicate);
        cacheService.removeIf(templatePredicate);
        cacheServiceList.forEach(pluginCacheService -> pluginCacheService.removeIf(templatePredicate));
    }
}
