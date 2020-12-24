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
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
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

@Service("checkDatabaseExecutor")
public class CheckDatabaseExecutor implements CheckExecutor {

    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;
    private final DatamartDao datamartDao;

    @Autowired
    public CheckDatabaseExecutor(DataSourcePluginService dataSourcePluginService,
                                 EntityDao entityDao,
                                 DatamartDao datamartDao) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.entityDao = entityDao;
        this.datamartDao = datamartDao;
    }

    @Override
    public Future<String> execute(CheckContext context) {
        String datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return datamartExists(datamartMnemonic)
            .compose(exist -> exist ? entityDao.getEntityNamesByDatamart(datamartMnemonic)
                    : Future.failedFuture(new DatamartNotExistsException(datamartMnemonic)))
            .compose(names -> getEntities(names, datamartMnemonic))
            .compose(entities -> checkEntities(entities, context));
    }

    private Future<Boolean> datamartExists(String datamart) {
        return Future.future(promise -> {
            datamartDao.getDatamart(datamart)
                    .onSuccess(success -> promise.complete(true))
                    .onFailure(error -> promise.complete(false));
        });
    }

    private Future<List<Entity>> getEntities(List<String> entityNames, String datamartMnemonic) {
        return Future.future(promise -> CompositeFuture.join(
            entityNames.stream()
                .map(name -> entityDao.getEntity(datamartMnemonic, name))
                .collect(Collectors.toList())
        )
            .onSuccess(result -> promise.complete(result.list()))
            .onFailure(promise::fail));
    }

    private Future<String> checkEntities(List<Entity> entities, CheckContext context) {
        return Future.future(promise -> CompositeFuture.join(dataSourcePluginService.getSourceTypes()
            .stream()
            .map(type -> checkEntitiesBySourceType(entities, context, type))
            .collect(Collectors.toList()))
            .onSuccess(result -> {
                List<Pair<SourceType, List<String>>> list = result.list();
                if (list.stream().allMatch(pair -> pair.getValue().isEmpty())) {
                    promise.complete(String.format("Datamart %s (%s) is ok.",
                        context.getRequest().getQueryRequest().getDatamartMnemonic(),
                        list.stream()
                            .map(pair -> pair.getKey().name())
                            .collect(Collectors.joining(", "))));
                } else {
                    String errors = list.stream()
                        .map(pair -> String.format("%s : %s", pair.getKey(),
                            pair.getValue().isEmpty()
                                ? "ok"
                                : String.join("", pair.getValue())))
                        .collect(Collectors.joining("\n"));
                    promise.complete(String.format("Datamart '%s' check failed!\n%s",
                        context.getRequest().getQueryRequest().getDatamartMnemonic(), errors));
                }
            })
            .onFailure(promise::fail));
    }

    private Future<Pair<SourceType, List<String>>> checkEntitiesBySourceType(List<Entity> entities,
                                                                             CheckContext context,
                                                                             SourceType sourceType) {
        return Future.future(promise -> CompositeFuture.join(entities.stream()
            .filter(entity -> entity.getDestination() == null || entity.getDestination().contains(sourceType))
            .filter(entity -> EntityType.TABLE.equals(entity.getEntityType()))
            .map(entity -> checkEntity(sourceType, new CheckContext(context.getMetrics(), context.getRequest(), entity)))
            .collect(Collectors.toList()))
            .onSuccess(result -> {
                List<Optional<String>> list = result.list();
                List<String> errors = list.stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());

                promise.complete(new Pair<>(sourceType, errors));
            })
            .onFailure(promise::fail));
    }

    private Future<Optional<String>> checkEntity(SourceType type, CheckContext context) {
        return Future.future(promise -> dataSourcePluginService
            .checkTable(type, context)
            .onSuccess(result -> promise.complete(Optional.empty()))
            .onFailure(fail -> {
                if (fail instanceof CheckException) {
                    promise.complete(Optional.of(
                        String.format("\n`%s` entity :%s", context.getEntity().getName(),
                            fail.getMessage())));
                } else {
                    promise.fail(fail);
                }
            }));
    }

    @Override
    public CheckType getType() {
        return CheckType.DATABASE;
    }
}
