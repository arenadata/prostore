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
package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DeltaQueryParamExtractorImpl implements DeltaQueryParamExtractor {

    private final DefinitionService<SqlNode> definitionService;
    private final DeltaQueryFactory deltaQueryFactory;
    private final Vertx coreVertx;

    @Autowired
    public DeltaQueryParamExtractorImpl(
            @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
            DeltaQueryFactory deltaQueryFactory, Vertx coreVertx) {
        this.definitionService = definitionService;
        this.deltaQueryFactory = deltaQueryFactory;
        this.coreVertx = coreVertx;
    }

    @Override
    public void extract(QueryRequest request, Handler<AsyncResult<DeltaQuery>> handler) {
        coreVertx.executeBlocking(it -> {
            try {
                SqlNode node = definitionService.processingQuery(request.getSql());
                it.complete(node);
            } catch (Exception e) {
                log.error("Request parsing error", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                SqlNode sqlNode = (SqlNode) ar.result();
                try {
                    final DeltaQuery deltaQuery = deltaQueryFactory.create(sqlNode);
                    log.debug("Delta query created successfully: {}", deltaQuery);
                    handler.handle(Future.succeededFuture(deltaQuery));
                } catch (Exception e) {
                    log.error("Error creating delta query from sql node", e);
                    handler.handle(Future.failedFuture(e));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
