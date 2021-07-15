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
package io.arenadata.dtm.query.execution.core.kafka;

import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class StatusMonitorServiceImpl implements StatusMonitorService {

    @Autowired
    @Qualifier("itTestWebClient")
    private WebClient webClient;

    @Override
    public Future<StatusResponse> getTopicStatus(String host, int port, StatusRequest statusRequest) {
        return Future.future(promise ->
                webClient.post(port, host, "/status")
                        .sendJson(statusRequest, ar -> {
                            if (ar.succeeded()) {
                                final HttpResponse<Buffer> httpResponse = ar.result();
                                promise.complete(httpResponse.bodyAsJson(StatusResponse.class));
                            } else {
                                promise.fail(ar.cause());
                            }
                        }));
    }
}
