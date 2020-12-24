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
package io.arenadata.dtm.query.execution.core.service.eddl.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.eddl.CreateUploadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlAction;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CreateUploadExternalTableExecutor implements EddlExecutor {

    private final DatamartDao datamartDao;
    private final EntityDao entityDao;

    @Autowired
    public CreateUploadExternalTableExecutor(ServiceDbFacade serviceDbFacade) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(EddlQuery query, Handler<AsyncResult<Void>> handler) {
        try {
            CreateUploadExternalTableQuery castQuery = (CreateUploadExternalTableQuery) query;
            val schema = castQuery.getSchemaName();
            val entity = castQuery.getEntity();
            entity.setExternalTableLocationType(ExternalTableLocationType.valueOf(castQuery.getLocationType().getName().toUpperCase()));
            entity.setExternalTableLocationPath(castQuery.getLocationPath());
            entity.setExternalTableFormat(castQuery.getFormat().getName());
            entity.setExternalTableSchema(castQuery.getTableSchema());
            entity.setExternalTableUploadMessageLimit(castQuery.getMessageLimit());
            datamartDao.existsDatamart(schema)
                    .compose(isExistsDatamart -> isExistsDatamart ?
                            entityDao.existsEntity(schema, entity.getName()) : Future.failedFuture(new DatamartNotExistsException(schema)))
                    .onSuccess(isExistsEntity -> createTableIfNotExists(entity, isExistsEntity)
                            .onSuccess(success -> handler.handle(Future.succeededFuture()))
                            .onFailure(fail -> handler.handle(Future.failedFuture(fail))))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error creating table by query request: {}!", query, e);
            handler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.CREATE_UPLOAD_EXTERNAL_TABLE;
    }

    private Future<Void> createTableIfNotExists(Entity entity, Boolean isTableExists) {
        if (isTableExists) {
            final RuntimeException existsException =
                    new RuntimeException(String.format("Table [%s] is already exists in datamart [%s]!",
                            entity.getName(),
                            entity.getSchema()));
            log.error("Error creating table [{}] in datamart [{}]!",
                    entity.getName(),
                    entity.getSchema(),
                    existsException);
            return Future.failedFuture(existsException);
        } else {
            return createTable(entity);
        }
    }

    private Future<Void> createTable(Entity entity) {
        return entityDao.createEntity(entity)
                .onSuccess(ar2 -> {
                    log.debug("Table [{}] in datamart [{}] successfully created",
                            entity.getName(),
                            entity.getSchema());
                })
                .onFailure(fail -> {
                    log.error("Error creating table [{}] in datamart [{}]!",
                            entity.getName(),
                            entity.getSchema(), fail);
                });
    }
}
