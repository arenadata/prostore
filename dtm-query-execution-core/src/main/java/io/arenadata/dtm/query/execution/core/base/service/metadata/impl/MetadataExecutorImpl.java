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
package io.arenadata.dtm.query.execution.core.base.service.metadata.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Component
public class MetadataExecutorImpl implements MetadataExecutor<DdlRequestContext> {

    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public MetadataExecutorImpl(DataSourcePluginService dataSourcePluginService) {
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context) {
        return Future.future(promise -> {
            List<Future> futures = new ArrayList<>();
            Set<SourceType> destination = Optional.ofNullable(context.getEntity())
                    .map(Entity::getDestination)
                    .filter(set -> !set.isEmpty())
                    .orElse(dataSourcePluginService.getSourceTypes());
            destination.forEach(sourceType ->
                    futures.add(dataSourcePluginService.ddl(
                            sourceType,
                            context.getMetrics(),
                            DdlRequest.builder()
                                    .datamartMnemonic(context.getDatamartName())
                                    .entity(context.getEntity())
                                    .envName(context.getEnvName())
                                    .requestId(context.getRequest().getQueryRequest().getRequestId())
                                    .sqlKind(context.getSqlNode().getKind())
                                    .build())
                    ));
            CompositeFuture.join(futures).setHandler(ar -> {
                if (ar.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }
}
