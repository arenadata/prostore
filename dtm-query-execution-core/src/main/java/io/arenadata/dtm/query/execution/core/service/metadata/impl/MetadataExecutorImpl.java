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
package io.arenadata.dtm.query.execution.core.service.metadata.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
    public void execute(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
        List<Future> futures = new ArrayList<>();
        Set<SourceType> destination = Optional.ofNullable(context.getRequest().getEntity())
                .map(Entity::getDestination)
                .filter(set -> !set.isEmpty())
                .orElse(dataSourcePluginService.getSourceTypes());
        destination.forEach(sourceType ->
                futures.add(Future.future(p -> dataSourcePluginService.ddl(
                        sourceType,
                        context,
                        ar -> {
                            if (ar.succeeded()) {
                                p.complete();
                            } else {
                                p.fail(ar.cause());
                            }
                        }))));
        CompositeFuture.join(futures).setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
