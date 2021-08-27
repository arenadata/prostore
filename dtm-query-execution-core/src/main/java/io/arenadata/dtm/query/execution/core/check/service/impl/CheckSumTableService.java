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
package io.arenadata.dtm.query.execution.core.check.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.check.dto.CheckSumRequestContext;
import io.arenadata.dtm.query.execution.core.check.exception.CheckSumException;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class CheckSumTableService {

    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;

    @Autowired
    public CheckSumTableService(DataSourcePluginService dataSourcePluginService, EntityDao entityDao) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.entityDao = entityDao;
    }

    public Future<Long> calcCheckSumForAllTables(CheckSumRequestContext request) {
        return entityDao.getEntityNamesByDatamart(request.getDatamart())
                .compose(entityNames -> getEntities(entityNames, request.getDatamart()))
                .compose(entities -> calcCheckSumForEntities(entities, request))
                .map(checkSumList -> checkSumList.stream()
                        .reduce(0L, Long::sum));
    }

    public Future<Long> calcCheckSumTable(CheckSumRequestContext request) {
        return CompositeFuture.join(request.getEntity().getDestination().stream()
                .map(sourceType -> checkSumInDatasource(sourceType, request))
                .collect(Collectors.toList()))
                .map(result -> {
                    List<Pair<SourceType, Long>> resultList = result.list();
                    long distinctCount = resultList.stream()
                            .map(Pair::getValue)
                            .distinct().count();
                    if (distinctCount == 1) {
                        return resultList.get(0).getValue();
                    } else {
                        val pluginResults = resultList.stream()
                                .map(pair -> String.format("%s : %s", pair.getKey(), pair.getValue()))
                                .collect(Collectors.joining("\n"));
                        throw new CheckSumException(request.getEntity().getName(), pluginResults);
                    }
                });
    }

    private Future<Pair<SourceType, Long>> checkSumInDatasource(SourceType sourceType, CheckSumRequestContext request) {
        return dataSourcePluginService.checkDataByHashInt32(sourceType,
                request.getCheckContext().getMetrics(),
                new CheckDataByHashInt32Request(
                        request.getCheckContext().getRequest().getQueryRequest().getRequestId(),
                        request.getCheckContext().getEnvName(),
                        request.getDatamart(),
                        request.getEntity(),
                        request.getCnFrom(),
                        request.getCnTo(),
                        getColumns(request),
                        request.getNormalization()))
                .map(result -> Pair.of(sourceType, result));
    }

    private Set<String> getColumns(CheckSumRequestContext request) {
        return request.getColumns() == null ? request.getEntity().getFields().stream()
                .map(EntityField::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new)) : request.getColumns();
    }

    private Future<List<Entity>> getEntities(List<String> entityNames, String datamartMnemonic) {
        return CompositeFuture.join(
                entityNames.stream()
                        .map(name -> entityDao.getEntity(datamartMnemonic, name))
                        .collect(Collectors.toList()))
                .map(result -> {
                    List<Entity> entities = result.list();
                    return entities.stream()
                            .filter(e -> e.getEntityType() == EntityType.TABLE)
                            .sorted(Comparator.comparing(Entity::getName))
                            .collect(Collectors.toList());
                });
    }

    private Future<List<Long>> calcCheckSumForEntities(List<Entity> entities, CheckSumRequestContext request) {
        List<Future> checkFutures = new ArrayList<>();
        entities.forEach(entity -> checkFutures.add(calcCheckSumTable(getNewRequestContext(entity, request))));
        return CompositeFuture.join(checkFutures)
                .map(CompositeFuture::list);
    }

    private CheckSumRequestContext getNewRequestContext(Entity entity, CheckSumRequestContext request) {
        val copyRequest = request.copy();
        copyRequest.setEntity(entity);
        return copyRequest;
    }
}
