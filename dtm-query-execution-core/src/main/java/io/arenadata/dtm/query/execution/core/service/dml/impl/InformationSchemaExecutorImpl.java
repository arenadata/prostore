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

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

@Service
public class InformationSchemaExecutorImpl implements InformationSchemaExecutor {

    private final SqlDialect coreSqlDialect;
    private final QueryParserService parserService;
    private final HSQLClient client;

    @Autowired
    public InformationSchemaExecutorImpl(HSQLClient client,
                                         @Qualifier("coreSqlDialect") SqlDialect coreSqlDialect,
                                         @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService) {
        this.client = client;
        this.coreSqlDialect = coreSqlDialect;
        this.parserService = parserService;
    }

    @Override
    public void execute(QuerySourceRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        toUpperCase(request);
        enrichmentSql(request)
            .onSuccess(query -> client.getQueryResult(query)
                .onSuccess(resultSet -> {
                    val result = resultSet.getRows().stream()
                        .map(JsonObject::getMap)
                        .collect(Collectors.toList());
                    asyncResultHandler.handle(Future.succeededFuture(
                        QueryResult.builder()
                            .requestId(request.getQueryRequest().getRequestId())
                            .metadata(request.getMetadata())
                            .result(result)
                            .build()));
                })
                .onFailure(r -> asyncResultHandler.handle(Future.failedFuture(r.getCause()))))
            .onFailure(error -> asyncResultHandler.handle(Future.failedFuture(error)));
    }

    private void toUpperCase(QuerySourceRequest request) {
        request.getMetadata()
            .forEach(c -> c.setName(c.getName().toUpperCase()));
    }

    Future<String> enrichmentSql(QuerySourceRequest request) {
        return Future.future(p -> {
                val parserRequest = new QueryParserRequest(request.getQueryRequest(), request.getLogicalSchema());
                parserService.parse(parserRequest, ar -> {
                    if (ar.succeeded()) {
                        val enrichmentNode = ar.result().getSqlNode();
                        val enrichmentSql = enrichmentNode.toSqlString(coreSqlDialect).getSql()
                                .replace(InformationSchemaView.SCHEMA_NAME.toLowerCase(), InformationSchemaView.DTM_SCHEMA_NAME.toLowerCase());
                        p.complete(enrichmentSql);
                    } else {
                        p.fail(ar.cause());
                    }
                });
            }
        );
    }
}
