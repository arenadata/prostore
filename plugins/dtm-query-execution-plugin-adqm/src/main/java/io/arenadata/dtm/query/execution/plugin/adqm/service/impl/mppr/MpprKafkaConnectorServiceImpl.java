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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.ConnectorProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.MpprKafkaConnectorService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.net.HttpURLConnection;

@Service
public class MpprKafkaConnectorServiceImpl implements MpprKafkaConnectorService {

    private final static Logger LOGGER = LoggerFactory.getLogger(MpprKafkaConnectorServiceImpl.class);
    private final ConnectorProperties connectorProperties;
    private final WebClient client;

    @Autowired
    public MpprKafkaConnectorServiceImpl(ConnectorProperties connectorProperties,
                                         @Qualifier("adqmWebClient") WebClient webClient) {
        this.connectorProperties = connectorProperties;
        this.client = webClient;
    }

    @Override
    public void call(MpprKafkaConnectorRequest request, Handler<AsyncResult<QueryResult>> handler) {
        LOGGER.debug("Calling MpprKafkaConnector with parameters: host = {}, port = {}, url = {}, request = {}",
                connectorProperties.getHost(), connectorProperties.getPort(), connectorProperties.getUrl(), request);
        client.post(connectorProperties.getPort(),
                connectorProperties.getHost(),
                connectorProperties.getUrl())
                .sendJson(request, ar -> {
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == HttpURLConnection.HTTP_OK) {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        } else {
                            LOGGER.error("Request execution error [{}]", request);
                            handler.handle(Future.failedFuture(ar.result().bodyAsString()));
                        }
                    } else {
                        LOGGER.error("Query execution error [{}]", request);
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
