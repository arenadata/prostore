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
package io.arenadata.dtm.query.execution.core.dml.service;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dml.dto.PluginDeterminationRequest;
import io.arenadata.dtm.query.execution.core.dml.dto.PluginDeterminationResult;
import io.arenadata.dtm.query.execution.core.query.exception.QueriedEntityIsMissingException;
import io.vertx.core.Future;
import lombok.val;
import lombok.var;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
public class PluginDeterminationService {
    private final SuitablePluginSelector suitablePluginSelector;
    private final SelectCategoryQualifier selectCategoryQualifier;
    private final ShardingCategoryQualifier shardingCategoryQualifier;
    private final AcceptableSourceTypesDefinitionService acceptableSourceTypesDefinitionService;

    public PluginDeterminationService(SuitablePluginSelector suitablePluginSelector,
                                      SelectCategoryQualifier selectCategoryQualifier,
                                      ShardingCategoryQualifier shardingCategoryQualifier,
                                      AcceptableSourceTypesDefinitionService acceptableSourceTypesDefinitionService) {
        this.suitablePluginSelector = suitablePluginSelector;
        this.selectCategoryQualifier = selectCategoryQualifier;
        this.shardingCategoryQualifier = shardingCategoryQualifier;
        this.acceptableSourceTypesDefinitionService = acceptableSourceTypesDefinitionService;
    }

    public Future<PluginDeterminationResult> determine(PluginDeterminationRequest request) {
        return getAcceptablePlugins(request)
                .map(acceptablePlugins -> {
                    var mostSuitablePlugin = request.getCachedMostSuitablePlugin();
                    if (mostSuitablePlugin == null) {
                        val category = selectCategoryQualifier.qualify(request.getSchema(), request.getQuery());
                        val shardingCategory = shardingCategoryQualifier.qualify(request.getSchema(), request.getQuery());
                        mostSuitablePlugin = suitablePluginSelector.selectByCategory(category, shardingCategory, acceptablePlugins)
                                .orElse(null);
                    }

                    var executionPlugin = request.getPreferredSourceType();
                    if (executionPlugin != null && !acceptablePlugins.contains(executionPlugin)) {
                        throw new QueriedEntityIsMissingException(executionPlugin);
                    } else if (executionPlugin == null) {
                        executionPlugin = mostSuitablePlugin;
                    }

                    return new PluginDeterminationResult(acceptablePlugins, mostSuitablePlugin, executionPlugin);
                });
    }

    private Future<Set<SourceType>> getAcceptablePlugins(PluginDeterminationRequest request) {
        if (request.getCachedAcceptablePlugins() != null && !request.getCachedAcceptablePlugins().isEmpty()) {
            return Future.succeededFuture(request.getCachedAcceptablePlugins());
        }

        return acceptableSourceTypesDefinitionService.define(request.getSchema());
    }

}
