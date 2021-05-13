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
package io.arenadata.dtm.query.execution.plugin.api.mppw.kafka;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.UUID;

@Getter
public class MppwKafkaRequest extends MppwRequest {
    private final List<KafkaBrokerInfo> brokers;
    private final String topic;

    @Builder(toBuilder = true)
    public MppwKafkaRequest(UUID requestId,
                            String envName,
                            String datamartMnemonic,
                            Boolean isLoadStart,
                            Entity sourceEntity,
                            Long sysCn,
                            String destinationTableName,
                            BaseExternalEntityMetadata uploadMetadata,
                            List<KafkaBrokerInfo> brokers,
                            String topic) {
        super(requestId,
                envName,
                datamartMnemonic,
                isLoadStart,
                sourceEntity,
                sysCn,
                destinationTableName,
                uploadMetadata,
                ExternalTableLocationType.KAFKA);
        this.brokers = brokers;
        this.topic = topic;
    }
}
