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

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AlterViewDdlExecutor extends CreateViewDdlExecutor {

    public static final String ALTER_VIEW_QUERY_PATH = "ALTER_VIEW.SELECT";

    public AlterViewDdlExecutor(@Qualifier("entityCacheService") EntityCacheService entityCacheService,
                                MetadataExecutor<DdlRequestContext> metadataExecutor,
                                LogicalSchemaProvider logicalSchemaProvider,
                                ColumnMetadataService columnMetadataService,
                                ServiceDbFacade serviceDbFacade,
                                @Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        super(entityCacheService,
            metadataExecutor,
            logicalSchemaProvider,
            columnMetadataService,
            serviceDbFacade,
            sqlDialect);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        checkViewQuery(context)
            .compose(v -> getCreateViewContext(context))
            .onFailure(error -> handler.handle(Future.failedFuture(error)))
            .onSuccess(ctx -> {
                val viewEntity = ctx.getViewEntity();
                context.setDatamartName(viewEntity.getSchema());
                entityDao.getEntity(viewEntity.getSchema(), viewEntity.getName())
                    .map(this::checkEntityType)
                    .compose(r -> entityDao.updateEntity(viewEntity))
                    .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                    .onFailure(error -> handler.handle(Future.failedFuture(error)));
            });
    }

    @Override
    protected String getViewQuery(SqlSelectTree tree) {
        val queryByView = tree.findNodesByPath(ALTER_VIEW_QUERY_PATH);
        if (queryByView.isEmpty()) {
            throw new IllegalArgumentException("Unable to get view query");
        } else {
            return queryByView.get(0).getNode().toSqlString(sqlDialect).toString();
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.ALTER_VIEW;
    }

}
