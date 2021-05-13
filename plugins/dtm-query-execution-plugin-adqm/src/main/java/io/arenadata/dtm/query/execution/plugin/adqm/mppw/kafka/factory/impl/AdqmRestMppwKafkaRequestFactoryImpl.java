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
package io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adqm.mppw.configuration.properties.AdqmMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.dto.RestMppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.factory.AdqmRestMppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AdqmRestMppwKafkaRequestFactoryImpl implements AdqmRestMppwKafkaRequestFactory {

    private final AdqmMppwProperties adqmMppwProperties;

    @Autowired
    public AdqmRestMppwKafkaRequestFactoryImpl(AdqmMppwProperties adqmMppwProperties) {
        this.adqmMppwProperties = adqmMppwProperties;
    }

    @Override
    public RestMppwKafkaLoadRequest create(MppwKafkaRequest mppwPluginRequest) {
        val uploadMeta = (UploadExternalEntityMetadata)
                mppwPluginRequest.getUploadMetadata();
        return RestMppwKafkaLoadRequest.builder()
                .requestId(mppwPluginRequest.getRequestId().toString())
                .datamart(mppwPluginRequest.getDatamartMnemonic())
                .tableName(mppwPluginRequest.getDestinationTableName())
                .kafkaTopic(mppwPluginRequest.getTopic())
                .kafkaBrokers(mppwPluginRequest.getBrokers())
                .hotDelta(mppwPluginRequest.getSysCn())
                .consumerGroup(adqmMppwProperties.getRestLoadConsumerGroup())
                .format(uploadMeta.getFormat().getName())
                .schema(new Schema.Parser().parse(uploadMeta.getExternalSchema()))
                .messageProcessingLimit(uploadMeta.getUploadMessageLimit() == null ? 0 : uploadMeta.getUploadMessageLimit())
                .build();
    }
}
