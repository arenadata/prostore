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

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;
import org.tarantool.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service("coreCheckService")
public class CheckServiceImpl implements CheckService {
    private static final String CHECK_RESULT_COLUMN_NAME = "check_result";
    private final Map<CheckType, CheckExecutor> executorMap;

    public CheckServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    @Override
    public void execute(CheckContext context, Handler<AsyncResult<QueryResult>> handler) {
        String datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        if (StringUtils.isEmpty(datamart)) {
            handler.handle(Future.failedFuture(
                new IllegalArgumentException("Datamart must be specified for all tables and views")));
        } else {
            executorMap.get(context.getCheckType()).execute(context)
                .onSuccess(result -> handler.handle(Future.succeededFuture(
                    createQueryResult(context.getRequest().getQueryRequest().getRequestId(), result))))
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        }

    }

    @Override
    public void addExecutor(CheckExecutor executor) {
        executorMap.put(executor.getType(), executor);
    }

    private QueryResult createQueryResult(UUID requestId, String result) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(CHECK_RESULT_COLUMN_NAME, result);
        return QueryResult.builder()
            .requestId(requestId)
            .metadata(Collections.singletonList(ColumnMetadata.builder()
                .name(CHECK_RESULT_COLUMN_NAME)
                .type(ColumnType.VARCHAR)
                .build()))
            .result(Collections.singletonList(resultMap))
            .build();
    }
}
