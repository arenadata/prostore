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
package io.arenadata.dtm.query.execution.plugin.adp.status.service;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.query.execution.plugin.adp.base.properties.AdpMppwProperties;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adpStatusService")
public class AdpStatusService implements StatusService {
    private final KafkaConsumerMonitor kafkaConsumerMonitor;
    private final AdpMppwProperties mppwProperties;

    @Autowired
    public AdpStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor,
                            AdpMppwProperties mppwProperties) {
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<StatusQueryResult> execute(String topic) {
        return Future.future(promise -> {
            String consumerGroup = mppwProperties.getKafkaConsumerGroup();
            kafkaConsumerMonitor.getAggregateGroupConsumerInfo(consumerGroup, topic)
                    .onSuccess(kafkaInfoResult -> {
                        StatusQueryResult result = new StatusQueryResult();
                        result.setPartitionInfo(kafkaInfoResult);
                        promise.complete(result);
                    })
                    .onFailure(promise::fail);
        });
    }
}
