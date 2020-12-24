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
package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.service.QueryDispatcher;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class QueryDispatcherImpl implements QueryDispatcher {

    private final Map<SqlProcessingType, DatamartExecutionService<RequestContext<? extends DatamartRequest>, AsyncResult<QueryResult>>> serviceMap = new HashMap<>();

    @Autowired
    public QueryDispatcherImpl(List<DatamartExecutionService<? extends RequestContext<? extends DatamartRequest>, AsyncResult<QueryResult>>> services) {
        for (DatamartExecutionService<? extends RequestContext<? extends DatamartRequest>, AsyncResult<QueryResult>> es : services) {
            serviceMap.put(es.getSqlProcessingType(), (DatamartExecutionService<RequestContext<? extends DatamartRequest>, AsyncResult<QueryResult>>) es);
        }
    }

    @Override
    public void dispatch(RequestContext<?> context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        try {
            serviceMap.get(context.getProcessingType())
                    .execute(context, asyncResultHandler);
        } catch (Exception e) {
            log.error("An error occurred while dispatching the request", e);
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }
}
