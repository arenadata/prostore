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
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlAction;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.arenadata.dtm.query.execution.core.service.cache.CacheService;
import io.arenadata.dtm.query.execution.core.service.cache.key.EntityKey;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DropUploadExternalTableExecutor implements EddlExecutor {

    private final CacheService<EntityKey, Entity> entityCacheService;
    private final EntityDao entityDao;

    @Autowired
    public DropUploadExternalTableExecutor(@Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                           ServiceDbFacade serviceDbFacade) {
        this.entityCacheService = entityCacheService;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(EddlQuery query, Handler<AsyncResult<Void>> handler) {
        try {
            DropUploadExternalTableQuery castQuery = (DropUploadExternalTableQuery) query;
            val datamartName = castQuery.getSchemaName();
            val entityName = castQuery.getTableName();
            evictEntityCache(datamartName, entityName);
            dropTable(datamartName, entityName)
                .onSuccess(r -> handler.handle(Future.succeededFuture()))
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error deleting table!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.DROP_UPLOAD_EXTERNAL_TABLE;
    }

    protected Future<Void> dropTable(String datamartName, String entityName) {
        return getEntity(datamartName, entityName)
            .compose(this::dropEntityIfExists);
    }

    private void evictEntityCache(String datamartName, String entityName) {
        entityCacheService.remove(new EntityKey(datamartName, entityName));
    }

    private Future<Entity> getEntity(String datamartName, String entityName) {
        return Future.future(entityPromise -> {
            entityDao.getEntity(datamartName, entityName)
                .onSuccess(entity -> {
                    if (EntityType.UPLOAD_EXTERNAL_TABLE == entity.getEntityType()) {
                        entityPromise.complete(entity);
                    } else {
                        val errMsg = String.format("Table [%s] in datamart [%s] doesn't exist!", entityName, datamartName);
                        log.error(errMsg);
                        entityPromise.fail(errMsg);
                    }
                })
                .onFailure(error -> {
                    log.error("Table [{}] in datamart [{}] doesn't exist!", entityName, datamartName, error);
                    entityPromise.fail(error);
                });
        });
    }

    private Future<Void> dropEntityIfExists(Entity entity) {
        if (entity != null) {
            return entityDao.deleteEntity(entity.getSchema(), entity.getName());
        } else {
            return Future.succeededFuture();
        }
    }
}
