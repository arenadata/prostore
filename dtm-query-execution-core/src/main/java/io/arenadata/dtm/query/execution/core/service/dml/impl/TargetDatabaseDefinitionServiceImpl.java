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
package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.TargetDatabaseDefinitionService;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.vertx.core.*;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.newHashSet;

@Service
public class TargetDatabaseDefinitionServiceImpl implements TargetDatabaseDefinitionService {

    private final DataSourcePluginService pluginService;
    private final EntityDao entityDao;
    private final DtmConfig dtmSettings;

    @Autowired
    public TargetDatabaseDefinitionServiceImpl(DataSourcePluginService pluginService,
                                               EntityDao entityDao,
                                               DtmConfig dtmSettings) {
        this.pluginService = pluginService;
        this.entityDao = entityDao;
        this.dtmSettings = dtmSettings;
    }

    @Override
    public void getTargetSource(QuerySourceRequest request, Handler<AsyncResult<QuerySourceRequest>> handler) {
        getEntitiesSourceTypes(request)
            .compose(entities -> defineTargetSourceType(entities, request))
            .map(sourceType -> {
                val queryRequestWithSourceType = request.getQueryRequest().copy();
                queryRequestWithSourceType.setSourceType(sourceType);
                return QuerySourceRequest.builder()
                    .queryRequest(queryRequestWithSourceType)
                    .logicalSchema(request.getLogicalSchema())
                    .metadata(request.getMetadata())
                    .sourceType(sourceType)
                    .build();
            })
            .onComplete(handler);
    }

    private Future<List<Entity>> getEntitiesSourceTypes(QuerySourceRequest request) {
        return Future.future(promise -> {
            List<Future> entityFutures = new ArrayList<>();
            request.getLogicalSchema().forEach(datamart ->
                    datamart.getEntities().forEach(entity ->
                            entityFutures.add(entityDao.getEntity(datamart.getMnemonic(), entity.getName()))
                    ));

            CompositeFuture.join(entityFutures)
                .onSuccess(entities -> promise.complete(entities.list()))
                .onFailure(promise::fail);
        });
    }

    private Future<SourceType> defineTargetSourceType(List<Entity> entities, QuerySourceRequest request) {
        return Future.future((Promise<SourceType> promise) -> {
            Set<SourceType> sourceTypes = getSourceTypes(request, entities);
            if (sourceTypes.size() == 1) {
                promise.complete(sourceTypes.iterator().next());
            } else {
                getTargetSourceByCalcQueryCost(sourceTypes, request)
                    .onComplete(promise);
            }
        });
    }

    private Set<SourceType> getSourceTypes(QuerySourceRequest request, List<Entity> entities) {
        final Set<SourceType> stResult = getCommonSourceTypes(entities);
        if (stResult.isEmpty()) {
            throw new RuntimeException("Tables have no datasource in common");
        } else if (request.getSourceType() != null) {
            if (!stResult.contains(request.getSourceType())) {
                throw new RuntimeException(String.format("Tables common datasources does not include %s",
                    request.getSourceType()));
            } else {
                return newHashSet(request.getSourceType());
            }
        } else {
            return stResult;
        }
    }

    private Set<SourceType> getCommonSourceTypes(List<Entity> entities) {
        final Set<SourceType> stResult = new HashSet<>();
        entities.forEach(e -> {
            if (stResult.isEmpty()) {
                stResult.addAll(e.getDestination());
            } else {
                stResult.retainAll(e.getDestination());
            }
        });
        return stResult;
    }

    private Future<SourceType> getTargetSourceByCalcQueryCost(Set<SourceType> sourceTypes, QuerySourceRequest request) {
        return Future.future(promise -> CompositeFuture.join(sourceTypes.stream()
            .map(sourceType -> calcQueryCostInPlugin(request, sourceType))
            .collect(Collectors.toList()))
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    SourceType sourceType = ar.result().list().stream()
                        .map(res -> (Pair<SourceType, Integer>) res)
                        .min(Comparator.comparingInt(Pair::getValue))
                        .map(Pair::getKey)
                        .orElse(null);
                    promise.complete(sourceType);
                } else {
                    promise.fail(ar.cause());
                }
            }));
    }

    private Future<Object> calcQueryCostInPlugin(QuerySourceRequest request, SourceType sourceType) {
        return Future.future(p -> {
            val costRequest = new QueryCostRequest(request.getQueryRequest(), request.getLogicalSchema());
            val costRequestContext = new QueryCostRequestContext(
                    createRequestMetrics(request),
                    costRequest);
            pluginService.calcQueryCost(sourceType, costRequestContext, costHandler -> {
                if (costHandler.succeeded()) {
                    p.complete(Pair.of(sourceType, costHandler.result()));
                } else {
                    p.fail(costHandler.cause());
                }
            });
        });
    }

    private RequestMetrics createRequestMetrics(QuerySourceRequest request) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(dtmSettings.getTimeZone()))
                .requestId(request.getQueryRequest().getRequestId())
                .sourceType(SourceType.INFORMATION_SCHEMA)
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }
}
