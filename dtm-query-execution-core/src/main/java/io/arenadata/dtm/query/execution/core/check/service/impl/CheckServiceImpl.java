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
package io.arenadata.dtm.query.execution.core.check.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.check.service.CheckExecutor;
import io.arenadata.dtm.query.execution.core.check.service.CheckService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;
import org.tarantool.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Service("coreCheckService")
public class CheckServiceImpl implements CheckService {
    private final Map<CheckType, CheckExecutor> executorMap;

    public CheckServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        String datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        if (isDatamartRequired(context) && StringUtils.isEmpty(datamart)) {
            return Future.failedFuture(
                    new DtmException("Datamart must be specified for all tables and views"));
        } else {
            return executorMap.get(context.getCheckType())
                    .execute(context);
        }
    }

    private boolean isDatamartRequired(CheckContext context) {
        switch (context.getCheckType()) {
            case VERSIONS:
            case MATERIALIZED_VIEW:
            case CHANGES:
                return false;
            default:
                return true;
        }
    }

    @Override
    public void addExecutor(CheckExecutor executor) {
        executorMap.put(executor.getType(), executor);
    }

}
