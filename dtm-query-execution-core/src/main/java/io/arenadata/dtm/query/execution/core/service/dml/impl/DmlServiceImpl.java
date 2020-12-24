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

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("coreDmlService")
public class DmlServiceImpl implements DmlService<QueryResult> {
    private final Map<SqlKind, DmlExecutor<QueryResult>> executorMap;

    public DmlServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    @Override
    public void execute(DmlRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        executorMap.get(context.getQuery().getKind()).execute(context, handler);
    }

    @Override
    public void addExecutor(DmlExecutor<QueryResult> executor) {
        executorMap.put(executor.getSqlKind(), executor);
    }
}
