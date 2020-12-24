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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.rollback;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.AdqmQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service("adqmRollbackService")
public class AdqmRollbackService implements RollbackService<Void> {
    private final RollbackRequestFactory<AdqmRollbackRequest> rollbackRequestFactory;
    private final AdqmQueryExecutor adqmQueryExecutor;

    @Autowired
    public AdqmRollbackService(RollbackRequestFactory<AdqmRollbackRequest> rollbackRequestFactory,
                               AdqmQueryExecutor adqmQueryExecutor) {
        this.rollbackRequestFactory = rollbackRequestFactory;
        this.adqmQueryExecutor = adqmQueryExecutor;
    }

    @Override
    public void execute(RollbackRequestContext context, Handler<AsyncResult<Void>> handler) {
        try {
            val rollbackRequest = rollbackRequestFactory.create(context.getRequest());
            Future<Void> executingFuture = Future.succeededFuture();
            for (PreparedStatementRequest statement : rollbackRequest.getStatements()) {
               executingFuture = executingFuture.compose(v -> executeSql(statement.getSql()));
            }
            executingFuture.onSuccess(success -> handler.handle(Future.succeededFuture()))
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Rollback error while executing context: [{}]: {}", context, e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Void> executeSql(String sql) {
        return Future.future(p -> adqmQueryExecutor.executeUpdate(sql, ar -> {
            if (ar.succeeded()) {
                p.complete();
            } else {
                log.error("Rollback error while executing sql: [{}]: {}", sql, ar.cause());
                p.fail(ar.cause());
            }
        }));
    }
}
