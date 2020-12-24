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

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwKafkaLoadRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class MppwKafkaLoadRequestFactoryImpl implements MppwKafkaLoadRequestFactory {

    private final List<String> excludeSystemFields = Arrays.asList(MetadataSqlFactoryImpl.SYS_FROM_ATTR, MetadataSqlFactoryImpl.SYS_TO_ATTR);

    @Override
    public MppwKafkaLoadRequest create(MppwRequestContext context, String server, MppwProperties mppwProperties) {
        val uploadMeta = (UploadExternalEntityMetadata) context.getRequest()
            .getKafkaParameter().getUploadMetadata();
        val kafkaParam = context.getRequest().getKafkaParameter();
        val schema = new Schema.Parser().parse(uploadMeta.getExternalSchema());
        val reqId = context.getRequest().getQueryRequest().getRequestId().toString();
        return MppwKafkaLoadRequest.builder()
            .requestId(reqId)
            .datamart(kafkaParam.getDatamart())
            .tableName(kafkaParam.getDestinationTableName())
            .writableExtTableName(MetadataSqlFactoryImpl.WRITABLE_EXT_TABLE_PREF + reqId)
            .columns(getColumns(schema))
            .schema(schema)
            .brokers(context.getRequest().getKafkaParameter().getBrokers().stream().map(KafkaBrokerInfo::getAddress).collect(Collectors.joining(",")))
            .consumerGroup(mppwProperties.getConsumerGroup())
            .timeout(mppwProperties.getStopTimeoutMs())
            .topic(context.getRequest().getKafkaParameter().getTopic())
            .uploadMessageLimit(mppwProperties.getDefaultMessageLimit())
            .server(server)
            .build();
    }

    private List<String> getColumns(Schema schema) {
        return schema.getFields().stream()
            .map(Schema.Field::name)
            .filter(field -> excludeSystemFields.stream()
                .noneMatch(sysName -> sysName.equals(field)))
            .collect(Collectors.toList());
    }
}
