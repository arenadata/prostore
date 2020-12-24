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
import io.arenadata.dtm.query.calcite.core.extension.ddl.truncate.SqlBaseTruncate;
import io.arenadata.dtm.query.execution.core.utils.ParseQueryUtils;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlPostExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service("coreDdlService")
public class DdlServiceImpl implements DdlService<QueryResult> {

    private final Map<SqlKind, DdlExecutor<QueryResult>> executorMap;
    private final Map<PostSqlActionType, DdlPostExecutor> postExecutorMap;
    private final ParseQueryUtils parseQueryUtils;

    @Autowired
    public DdlServiceImpl(ParseQueryUtils parseQueryUtils) {
        this.parseQueryUtils = parseQueryUtils;
        this.executorMap = new HashMap<>();
        this.postExecutorMap = new HashMap<>();
    }

    @Override
    public void execute(DdlRequestContext context,
                        Handler<AsyncResult<QueryResult>> handler) {
        try {
            SqlCall sqlCall = getSqlCall(context.getQuery());
            DdlExecutor<QueryResult> executor = executorMap.get(sqlCall.getKind());
            if (executor != null) {
                executor.execute(context, parseQueryUtils.getDatamartName(sqlCall.getOperandList()), ddlAr -> {
                    if (ddlAr.succeeded()) {
                        handler.handle(Future.succeededFuture(ddlAr.result()));
                        context.getPostActions().addAll(executor.getPostActions());

                    } else {
                        handler.handle(Future.failedFuture(ddlAr.cause()));
                    }
                    executePostActions(context);
                });
            } else {
                String error = String.format("Not supported DDL query type [%s]", context.getQuery());
                log.error(error);
                handler.handle(Future.failedFuture(error));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            handler.handle(Future.failedFuture(String.format("Not supported request type [%s]", context.getQuery())));
        }
    }

    private void executePostActions(DdlRequestContext context) {
        CompositeFuture.join(context.getPostActions().stream()
                .distinct()
                .map(postType -> Optional.ofNullable(postExecutorMap.get(postType))
                        .map(postExecutor -> postExecutor.execute(context))
                        .orElse(Future.failedFuture(String.format("Not supported DDL post executor type [%s]",
                                postType))))
                .collect(Collectors.toList()))
                .onFailure(error -> log.error(error.getMessage()));
    }

    private SqlCall getSqlCall(SqlNode sqlNode) {
        if (sqlNode instanceof SqlAlter || sqlNode instanceof SqlDdl || sqlNode instanceof SqlBaseTruncate) {
            return (SqlCall) sqlNode;
        } else {
            throw new RuntimeException("Not supported request type");
        }
    }

    @Override
    public void addExecutor(DdlExecutor<QueryResult> executor) {
        executorMap.put(executor.getSqlKind(), executor);
    }

    @Override
    public void addPostExecutor(DdlPostExecutor executor) {
        postExecutorMap.put(executor.getPostActionType(), executor);
    }
}
