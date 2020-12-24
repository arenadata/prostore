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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl.mppw;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.AdgMppwKafkaProperties;
import io.arenadata.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgMppwKafkaContextFactory;
import io.arenadata.dtm.query.execution.plugin.adg.model.callback.function.TtTransferDataScdCallbackFunction;
import io.arenadata.dtm.query.execution.plugin.adg.model.callback.params.TtTransferDataScdCallbackParameter;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.TtSubscriptionKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.TtTransferDataEtlRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.MppwKafkaService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service("adgMppwKafkaService")
public class AdgMppwKafkaService implements MppwKafkaService<QueryResult> {

    private final AdgMppwKafkaContextFactory contextFactory;
    private final Map<String, String> initializedLoadingByTopic;
    private final AdgMppwKafkaProperties properties;
    private final AdgCartridgeClient cartridgeClient;

    public AdgMppwKafkaService(AdgMppwKafkaContextFactory contextFactory,
                               AdgCartridgeClient cartridgeClient,
                               AdgMppwKafkaProperties properties) {
        this.contextFactory = contextFactory;
        this.cartridgeClient = cartridgeClient;
        this.properties = properties;
        initializedLoadingByTopic = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        log.debug("mppw start");
        val mppwKafkaContext = contextFactory.create(context.getRequest());
        if (context.getRequest().getIsLoadStart()) {
            initializeLoading(mppwKafkaContext, asyncResultHandler);
        } else {
            cancelLoadData(mppwKafkaContext, asyncResultHandler);
        }
    }

    private void initializeLoading(AdgMppwKafkaContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        if (initializedLoadingByTopic.containsKey(ctx.getTopicName())) {
            transferData(ctx, handler);
        } else {
            val callbackFunctionParameter = new TtTransferDataScdCallbackParameter(
                    ctx.getHelperTableNames().getStaging(),
                    ctx.getHelperTableNames().getStaging(),
                    ctx.getHelperTableNames().getActual(),
                    ctx.getHelperTableNames().getHistory(),
                    ctx.getHotDelta()
            );
            val callbackFunction = new TtTransferDataScdCallbackFunction(
                    properties.getCallbackFunctionName(),
                    callbackFunctionParameter,
                    properties.getMaxNumberOfMessagesPerPartition(),
                    properties.getCallbackFunctionSecIdle()
            );


            val request = new TtSubscriptionKafkaRequest(
                    properties.getMaxNumberOfMessagesPerPartition(),
                    null,
                    ctx.getTopicName(),
                    Collections.singletonList(ctx.getHelperTableNames().getStaging()),
                    callbackFunction
            );
            cartridgeClient.subscribe(request, ar -> {
                if (ar.succeeded()) {
                    log.debug("Loading initialize completed by [{}]", request);
                    initializedLoadingByTopic.put(ctx.getTopicName(), ctx.getConsumerTableName());
                    handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                } else {
                    log.error("Loading initialize error:", ar.cause());
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        }
    }

    private void transferData(AdgMppwKafkaContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        val request = new TtTransferDataEtlRequest(ctx.getHelperTableNames(), ctx.getHotDelta());
        cartridgeClient.transferDataToScdTable(
                request, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Transfer Data completed by request [{}]", request);
                        handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                    } else {
                        log.error("Transfer Data error: ", ar.cause());
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                }
        );
    }

    private void cancelLoadData(AdgMppwKafkaContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        val topicName = ctx.getTopicName();
        transferData(ctx, tr -> {
            cartridgeClient.cancelSubscription(topicName, ar -> {
                initializedLoadingByTopic.remove(topicName);
                if (ar.succeeded()) {
                    log.debug("Cancel Load Data completed by request [{}]", topicName);
                    handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                } else {
                    log.error("Cancel Load Data error: ", ar.cause());
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        });
    }
}
