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
package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.exception.entity.ViewNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.utils.SqlPreparer;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DropViewDdlExecutor extends QueryResultDdlExecutor {
    private final EntityCacheService entityCacheService;
    protected final EntityDao entityDao;

    @Autowired
    public DropViewDdlExecutor(@Qualifier("entityCacheService") EntityCacheService entityCacheService,
                               MetadataExecutor<DdlRequestContext> metadataExecutor,
                               ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        this.entityCacheService = entityCacheService;
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(DdlRequestContext context,
                        String sqlNodeName,
                        Handler<AsyncResult<QueryResult>> handler) {
        try {
            val tree = new SqlSelectTree(context.getQuery());
            val viewNameNode = SqlPreparer.getViewNameNode(tree);
            val schemaName = viewNameNode.tryGetSchemaName()
                .orElseThrow(() -> new RuntimeException("Unable to get schema of view"));
            val viewName = viewNameNode.tryGetTableName()
                .orElseThrow(() -> new RuntimeException("Unable to get name of view"));
            context.setDatamartName(schemaName);
            entityCacheService.remove(schemaName, viewName);
            entityDao.getEntity(schemaName, viewName)
                .compose(this::checkEntityType)
                .compose(v -> entityDao.deleteEntity(schemaName, viewName))
                .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                .onFailure(error -> handler.handle(Future.failedFuture(error)));
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Void> checkEntityType(Entity entity) {
        if (EntityType.VIEW == entity.getEntityType()) {
            return Future.succeededFuture();
        } else {
            return Future.failedFuture(new ViewNotExistsException(entity.getName()));
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_VIEW;
    }
}
