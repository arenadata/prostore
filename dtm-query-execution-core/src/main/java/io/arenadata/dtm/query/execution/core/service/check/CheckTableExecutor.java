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
package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckTable;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckException;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service("checkTableExecutor")
public class CheckTableExecutor implements CheckExecutor {
    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;

    @Autowired
    public CheckTableExecutor(DataSourcePluginService dataSourcePluginService, EntityDao entityDao) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.entityDao = entityDao;
    }

    @Override
    public Future<String> execute(CheckContext context) {
        String tableName = ((SqlCheckTable) context.getSqlCheckCall()).getTable();
        String datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return entityDao.getEntity(datamartMnemonic, tableName)
                .compose(entity -> {
                    if (EntityType.TABLE.equals(entity.getEntityType())) {
                        return Future.succeededFuture(entity);
                    } else {
                        return Future.failedFuture(String.format("%s.%s doesn't exist", datamartMnemonic, tableName));
                    }
                })
                .compose(entity -> checkEntity(entity, context));
    }

    private Future<String> checkEntity(Entity entity, CheckContext context) {
        return Future.future(promise -> CompositeFuture.join(entity.getDestination()
                .stream()
                .map(type -> checkEntityByType(new CheckContext(context.getMetrics(), context.getRequest(), entity), type))
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Pair<SourceType, Optional<String>>> list = result.list();
                    if (list.stream().map(Pair::getValue).noneMatch(Optional::isPresent)) {
                        promise.complete(String.format("Table %s.%s (%s) is ok.",
                                entity.getSchema(), entity.getName(),
                                list.stream()
                                        .map(pair -> pair.getKey().name())
                                        .collect(Collectors.joining(", "))));
                    } else {
                        String errors = list.stream()
                                .map(pair -> String.format("%s : %s", pair.getKey(), pair.getValue().orElse("ok")))
                                .collect(Collectors.joining("\n"));
                        promise.complete(String.format("Table '%s.%s' check failed!\n%s", entity.getSchema(),
                                entity.getName(), errors));
                    }
                })
                .onFailure(promise::fail));
    }

    private Future<Pair<SourceType, Optional<String>>> checkEntityByType(CheckContext context, SourceType type) {
        return Future.future(promise -> dataSourcePluginService
                .checkTable(type, context)
                .onSuccess(result -> promise.complete(new Pair<>(type, Optional.empty())))
                .onFailure(fail -> {
                    if (fail instanceof CheckException) {
                        promise.complete(new Pair<>(type, Optional.of(fail.getMessage())));
                    } else {
                        promise.fail(fail);
                    }
                }));
    }

    @Override
    public CheckType getType() {
        return CheckType.TABLE;
    }
}
