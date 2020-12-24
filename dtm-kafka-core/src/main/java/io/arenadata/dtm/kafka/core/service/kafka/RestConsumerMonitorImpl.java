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
package io.arenadata.dtm.kafka.core.service.kafka;

import io.arenadata.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.util.Date;

@Component
@Slf4j
public class RestConsumerMonitorImpl implements KafkaConsumerMonitor {
    private final WebClient webClient;
    private final KafkaProperties kafkaProperties;

    public RestConsumerMonitorImpl(@Qualifier("coreVertx") Vertx vertx,
                                   @Qualifier("coreKafkaProperties") KafkaProperties kafkaProperties) {
        this.webClient = WebClient.create(vertx);
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public Future<KafkaPartitionInfo> getAggregateGroupConsumerInfo(String consumerGroup, String topic) {
        return Future.future((Promise<KafkaPartitionInfo> p) -> {
            StatusRequest request = new StatusRequest(topic, consumerGroup);
            webClient.postAbs(kafkaProperties.getStatusMonitorUrl()).sendJsonObject(JsonObject.mapFrom(request), ar -> {
                if (ar.succeeded()) {
                    HttpResponse<Buffer> response = ar.result();
                    if (response.statusCode() < 400 && response.statusCode() >= 200) {
                        StatusResponse statusResponse;
                        try {
                            statusResponse = response.bodyAsJson(StatusResponse.class);
                        } catch (Exception e) {
                            p.fail(e);
                            return;
                        }
                        KafkaPartitionInfo kafkaPartitionInfo = KafkaPartitionInfo.builder()
                                .consumerGroup(statusResponse.getConsumerGroup())
                                .topic(statusResponse.getTopic())
                                .offset(statusResponse.getConsumerOffset())
                                .end(statusResponse.getProducerOffset())
                                .lastCommitTime(new Date(statusResponse.getLastCommitTime()).toInstant()
                                        .atZone(ZoneId.systemDefault()).toLocalDateTime())
                                .lastMessageTime(new Date(statusResponse.getLastMessageTime()).toInstant()
                                        .atZone(ZoneId.systemDefault()).toLocalDateTime())
                                .build();
                        p.complete(kafkaPartitionInfo);
                    } else {
                        p.fail(new RuntimeException(String.format("Received HTTP status %s, msg %s", response.statusCode(), response.bodyAsString())));
                    }
                } else {
                    p.fail(ar.cause());
                }
            });
        });
    }
}
