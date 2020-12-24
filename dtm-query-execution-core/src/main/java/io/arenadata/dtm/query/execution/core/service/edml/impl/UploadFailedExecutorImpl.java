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
package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.exception.CrashException;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class UploadFailedExecutorImpl implements EdmlUploadFailedExecutor {

    private final DeltaServiceDao deltaServiceDao;
    private final RollbackRequestContextFactory rollbackRequestContextFactory;
    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public UploadFailedExecutorImpl(DeltaServiceDao deltaServiceDao,
                                    RollbackRequestContextFactory rollbackRequestContextFactory,
                                    DataSourcePluginService dataSourcePluginService) {
        this.deltaServiceDao = deltaServiceDao;
        this.rollbackRequestContextFactory = rollbackRequestContextFactory;
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public Future<Void> execute(EdmlRequestContext context) {
        return Future.future(promise -> eraseWriteOp(context)
            .compose(v -> deltaServiceDao.deleteWriteOperation(context.getSourceEntity().getSchema(),
                context.getSysCn()))
            .setHandler(promise));
    }

    private Future<Void> eraseWriteOp(EdmlRequestContext context) {
        return Future.future(rbPromise -> {
            final RollbackRequestContext rollbackRequestContext =
                rollbackRequestContextFactory.create(context);
            eraseWriteOp(rollbackRequestContext)
                .onSuccess(rbPromise::complete)
                .onFailure(rbPromise::fail);
        });
    }

    @Override
    public Future<Void> eraseWriteOp(RollbackRequestContext context) {
        return Future.future(rbPromise -> {
            List<Future> futures = new ArrayList<>();
            final Set<SourceType> destination = context.getRequest().getEntity().getDestination().stream()
                    .filter(type -> dataSourcePluginService.getSourceTypes().contains(type))
                    .collect(Collectors.toSet());
            destination.forEach(sourceType ->
                futures.add(Future.future(p -> dataSourcePluginService.rollback(
                    sourceType,
                    context,
                    ar -> {
                        if (ar.succeeded()) {
                            log.debug("Rollback data in plugin [{}], datamart [{}], " +
                                    "table [{}], sysCn [{}] finished successfully",
                                sourceType,
                                context.getRequest().getDatamart(),
                                context.getRequest().getDestinationTable(),
                                context.getRequest().getSysCn());
                            p.complete();
                        } else {
                            log.error("Error rollback data in plugin [{}], " +
                                    "datamart [{}], table [{}], sysCn [{}]",
                                sourceType,
                                context.getRequest().getDatamart(),
                                context.getRequest().getDestinationTable(),
                                context.getRequest().getSysCn(),
                                ar.cause());
                            p.fail(ar.cause());
                        }
                    }))));
            CompositeFuture.join(futures).setHandler(ar -> {
                if (ar.succeeded()) {
                    rbPromise.complete();
                } else {
                    log.error("Error in rolling back data", ar.cause());
                    rbPromise.fail(
                        new CrashException("Error in rolling back data → Fatal error. Operation failed on execute and failed on undo.", ar.cause())
                    );
                }
            });
        });
    }
}
