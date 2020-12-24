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
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckData;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.verticle.TaskVerticleExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckException;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service("checkDataExecutor")
public class CheckDataExecutor implements CheckExecutor {
    private final DataSourcePluginService dataSourcePluginService;
    private final DeltaServiceDao deltaServiceDao;
    private final EntityDao entityDao;
    private final TaskVerticleExecutor taskVerticleExecutor;

    @Autowired
    public CheckDataExecutor(DataSourcePluginService dataSourcePluginService,
                             DeltaServiceDao deltaServiceDao, EntityDao entityDao,
                             TaskVerticleExecutor taskVerticleExecutor) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.deltaServiceDao = deltaServiceDao;
        this.entityDao = entityDao;
        this.taskVerticleExecutor = taskVerticleExecutor;
    }

    @Override
    public Future<String> execute(CheckContext context) {
        SqlCheckData sqlCheckData = (SqlCheckData) context.getSqlCheckCall();
        return entityDao.getEntity(context.getRequest().getQueryRequest().getDatamartMnemonic(), sqlCheckData.getTable())
                .compose(entity -> EntityType.TABLE.equals(entity.getEntityType())
                        ? Future.succeededFuture(entity)
                        : Future.failedFuture(new IllegalArgumentException(
                        String.format("Table `%s` not exist", sqlCheckData.getTable()))))
                .compose(entity -> check(context, entity, sqlCheckData));
    }

    @Override
    public CheckType getType() {
        return CheckType.DATA;
    }

    private Future<String> check(CheckContext context,
                                 Entity entity,
                                 SqlCheckData sqlCheckData) {

        return Future.future(promise -> check(sqlCheckData.getDeltaNum(), entity.getSchema(),
                getCheckFunc(context, entity, sqlCheckData.getColumns()))
                .onSuccess(result -> promise.complete(
                        String.format("Table '%s.%s' (%s) checksum for delta %s is Ok.",
                                entity.getSchema(), entity.getName(),
                                entity.getDestination().stream()
                                        .map(SourceType::name)
                                        .collect(Collectors.joining(", ")),
                                sqlCheckData.getDeltaNum())))
                .onFailure(exception -> {
                    if (exception instanceof CheckException) {
                        promise.complete(String.format("Table '%s.%s' checksum mismatch!\n%s",
                                entity.getSchema(), entity.getName(), exception.getMessage()));
                    } else {
                        promise.fail(exception);
                    }
                }));
    }

    private Future<Void> check(Long deltaNum,
                               String datamart,
                               Function<Long, Future<Void>> checkFunc) {
        return deltaServiceDao.getDeltaOk(datamart)
                .compose(deltaOk -> deltaServiceDao.getDeltaByNum(datamart, deltaNum)
                        .compose(delta -> Future.succeededFuture(new Pair<>(delta.getCnFrom(), deltaOk.getCnTo()))))
                .compose(checkRange -> checkByRange(checkRange, checkFunc));
    }

    private Future<Void> checkByRange(Pair<Long, Long> checkRange,
                                      Function<Long, Future<Void>> checkFunc) {
        return verticalCheck(checkRange.left, checkRange.right, checkFunc);
    }

    private Future<Void> verticalCheck(Long to,
                                       Long sysCn,
                                       Function<Long, Future<Void>> checkFunc) {
        if (sysCn < to) {
            return Future.succeededFuture();
        } else {
            return Future.future(promise -> taskVerticleExecutor.execute(p -> checkFunc.apply(sysCn)
                            .onSuccess(p::complete)
                            .onFailure(p::fail),
                    ar -> {
                        if (ar.succeeded()) {
                            verticalCheck(to, sysCn - 1, checkFunc)
                                    .onSuccess(promise::complete)
                                    .onFailure(promise::fail);
                        } else {
                            promise.fail(ar.cause());
                        }
                    }));
        }
    }

    private Function<Long, Future<Void>> getCheckFunc(CheckContext context,
                                                      Entity entity,
                                                      Set<String> columns) {
        BiFunction<SourceType, Long, Future<Long>> checkFunc = Optional.ofNullable(columns)
                .map(value -> getCheckHashFunc(context, entity, value))
                .orElse((type, sysCn) -> dataSourcePluginService.checkDataByCount(
                        new CheckDataByCountParams(type, context.getMetrics(), entity, sysCn,
                                context.getRequest().getQueryRequest().getEnvName())));
        return sysCn -> Future.future(promise -> CompositeFuture.join(
                entity.getDestination().stream()
                        .map(sourceType -> checkFunc.apply(sourceType, sysCn)
                                .compose(val -> Future.succeededFuture(new Pair<>(sourceType, val))))
                        .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Pair<SourceType, Long>> resultList = result.list();
                    if (resultList.stream().map(Pair::getValue).distinct().count() == 1) {
                        promise.complete();
                    } else {
                        promise.fail(new CheckException(resultList.stream()
                                .map(pair -> String.format("%s: %s", pair.getKey(), pair.getValue().toString()))
                                .collect(Collectors.joining("\n"))));
                    }
                })
                .onFailure(promise::fail));
    }

    private BiFunction<SourceType, Long, Future<Long>> getCheckHashFunc(CheckContext context,
                                                                        Entity entity,
                                                                        Set<String> columns) {
        Set<String> entityFieldNames = entity.getFields().stream()
                .map(EntityField::getName)
                .collect(Collectors.toSet());

        Set<String> notExistColumns = columns.stream()
                .filter(column -> !entityFieldNames.contains(column))
                .collect(Collectors.toSet());

        if (!notExistColumns.isEmpty()) {
            throw new IllegalArgumentException(String.format("Columns: `%s` don't exist.",
                    String.join(", ", notExistColumns)));
        } else {
            return (type, sysCn) -> dataSourcePluginService.checkDataByHashInt32(
                    new CheckDataByHashInt32Params(type, context.getMetrics(), entity, sysCn, columns,
                            context.getRequest().getQueryRequest().getEnvName()));
        }
    }
}
