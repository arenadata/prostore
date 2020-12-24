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
package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.adb.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MpprKafkaConnectorRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
public class MpprKafkaConnectorRequestFactoryImpl implements MpprKafkaConnectorRequestFactory {

    @Override
    public MpprKafkaConnectorRequest create(MpprRequest mpprRequest,
                                            String enrichedQuery) {
        QueryRequest queryRequest = mpprRequest.getQueryRequest();
        val downloadMetadata =
                (DownloadExternalEntityMetadata) mpprRequest.getKafkaParameter().getDownloadMetadata();
        return MpprKafkaConnectorRequest.builder()
                .table(queryRequest.getSql())
                .sql(enrichedQuery)
                .datamart(queryRequest.getDatamartMnemonic())
                .kafkaBrokers(mpprRequest.getKafkaParameter().getBrokers())
                .kafkaTopic(mpprRequest.getKafkaParameter().getTopic())
                .chunkSize(downloadMetadata.getChunkSize())
                .avroSchema(downloadMetadata.getExternalSchema())
                .metadata(mpprRequest.getMetadata())
                .sourceType(SourceType.ADB)
                .build();
    }
}
