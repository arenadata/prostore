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
package io.arenadata.dtm.query.execution.core.dml.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.query.exception.NoSingleDataSourceContainsAllEntitiesException;
import io.arenadata.dtm.query.execution.core.dml.service.AcceptableSourceTypesDefinitionService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class AcceptableSourceTypesDefinitionServiceImpl implements AcceptableSourceTypesDefinitionService {

    private final EntityDao entityDao;

    @Autowired
    public AcceptableSourceTypesDefinitionServiceImpl(EntityDao entityDao) {
        this.entityDao = entityDao;
    }

    @Override
    public Future<Set<SourceType>> define(QuerySourceRequest request) {
        return getEntities(request)
                .compose(entities -> Future.future(promise -> promise.complete(getSourceTypes(entities))));
    }

    private Future<List<Entity>> getEntities(QuerySourceRequest request) {
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

    private Set<SourceType> getSourceTypes(List<Entity> entities) {
        final Set<SourceType> stResult = getCommonSourceTypes(entities);
        if (stResult.isEmpty()) {
            throw new NoSingleDataSourceContainsAllEntitiesException();
        } else {
            return stResult;
        }
    }

    private Set<SourceType> getCommonSourceTypes(List<Entity> entities) {
        if (entities.isEmpty()) {
            return new HashSet<>();
        } else {
            Set<SourceType> stResult = new HashSet<>(entities.get(0).getDestination());
            entities.forEach(e -> stResult.retainAll(e.getDestination()));
            return stResult;
        }
    }
}
