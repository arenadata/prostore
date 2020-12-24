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
import io.arenadata.dtm.query.calcite.core.extension.ddl.truncate.SqlTruncateHistory;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.exception.entity.TableNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class TruncateDdlExecutor extends QueryResultDdlExecutor {

    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;
    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public TruncateDdlExecutor(DataSourcePluginService dataSourcePluginService,
                               DeltaServiceDao deltaServiceDao,
                               MetadataExecutor<DdlRequestContext> metadataExecutor,
                               ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        this.dataSourcePluginService = dataSourcePluginService;
        this.deltaServiceDao = deltaServiceDao;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        val schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
        context.setDatamartName(schema);
        val table = getTableName(sqlNodeName);
        val sqlTruncateHistory = (SqlTruncateHistory) context.getQuery();
        CompositeFuture.join(getTableEntity(schema, table), calcCnTo(schema, sqlTruncateHistory))
                .compose(entityCnTo -> CompositeFuture.join(executeTruncate(entityCnTo, context, sqlTruncateHistory)))
                .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                .onFailure(err -> handler.handle(Future.failedFuture(err)));
    }

    private List<Future> executeTruncate(CompositeFuture entityCnTo, DdlRequestContext context, SqlTruncateHistory sqlTruncateHistory) {
        val entity = (Entity) entityCnTo.resultAt(0);
        val cnTo = (Long) entityCnTo.resultAt(1);
        val env = context.getRequest().getQueryRequest().getEnvName();
        return entity.getDestination().stream()
                        .map(sourceType -> {
                            val truncateParams = new TruncateHistoryParams(sourceType,
                                    context.getMetrics(), cnTo, entity, env, sqlTruncateHistory.getConditions());
                            return dataSourcePluginService.truncateHistory(truncateParams);
                        })
                        .collect(Collectors.toList());
    }

    private Future<Entity> getTableEntity(String schema, String table) {
        return Future.future(p -> {
            entityDao.getEntity(schema, table)
                    .onSuccess(entity -> {
                        if (EntityType.TABLE.equals(entity.getEntityType())) {
                            p.complete(entity);
                        } else {
                            p.fail(new TableNotExistsException(table));
                        }
                    })
                    .onFailure(p::fail);
        });
    }

    private Future<Long> calcCnTo(String schema, SqlTruncateHistory truncateHistory) {
        return Future.future(p -> {
            if (truncateHistory.isInfinite()) {
                p.complete(null);
            } else {
                deltaServiceDao.getDeltaByDateTime(schema, truncateHistory.getDateTime())
                        .onSuccess(okDelta -> {
                            p.complete(okDelta.getCnTo());
                        })
                        .onFailure(p::fail);
            }
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.OTHER_DDL;
    }

    @Override
    public List<PostSqlActionType> getPostActions() {
        return Collections.singletonList(PostSqlActionType.PUBLISH_STATUS);
    }
}
