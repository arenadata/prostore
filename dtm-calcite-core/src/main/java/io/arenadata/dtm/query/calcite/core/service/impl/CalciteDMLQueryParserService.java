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
package io.arenadata.dtm.query.calcite.core.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;

@Slf4j
public abstract class CalciteDMLQueryParserService implements QueryParserService {
    private final CalciteContextProvider contextProvider;
    private final Vertx vertx;

    public CalciteDMLQueryParserService(CalciteContextProvider contextProvider,
                                        Vertx vertx) {
        this.contextProvider = contextProvider;
        this.vertx = vertx;
    }

    @Override
    public void parse(QueryParserRequest request, Handler<AsyncResult<QueryParserResponse>> asyncResultHandler) {
        vertx.executeBlocking(it -> {
            try {
                val context = contextProvider.context(extendSchemes(request.getSchema()));
                try {
                    val sql = request.getQueryRequest().getSql();
                    val parse = context.getPlanner().parse(sql);
                    val validatedQuery = context.getPlanner().validate(parse);
                    val relQuery = context.getPlanner().rel(validatedQuery);
                    val copyRequest = request.getQueryRequest().copy();
                    copyRequest.setSql(sql);
                    it.complete(new QueryParserResponse(
                        context,
                        copyRequest,
                        request.getSchema(),
                        relQuery,
                        parse
                    ));
                } catch (Exception e) {
                    log.error("Request parsing error", e);
                    it.fail(e);
                }
            } catch (Exception e) {
                log.error("Request parsing error", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                asyncResultHandler.handle(Future.succeededFuture((QueryParserResponse) ar.result()));
            } else {
                log.debug("Error while executing the parse method", ar.cause());
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    protected List<Datamart> extendSchemes(List<Datamart> datamarts) {
        return datamarts;
    }
}
