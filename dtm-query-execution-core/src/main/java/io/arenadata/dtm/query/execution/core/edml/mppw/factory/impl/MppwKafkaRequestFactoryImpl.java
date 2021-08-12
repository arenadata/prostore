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
package io.arenadata.dtm.query.execution.core.edml.mppw.factory.impl;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.kafka.core.repository.ZookeeperKafkaProviderRepository;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.exception.UnreachableLocationException;
import io.arenadata.dtm.query.execution.core.edml.mppw.factory.MppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.edml.service.LocationUriParser;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class MppwKafkaRequestFactoryImpl implements MppwKafkaRequestFactory {

    private final ZookeeperKafkaProviderRepository zkConnProviderRepository;
    private final Vertx vertx;
    private final LocationUriParser locationUriParser;

    @Autowired
    public MppwKafkaRequestFactoryImpl(@Qualifier("coreVertx") Vertx vertx,
                                       @Qualifier("mapZkKafkaProviderRepository") ZookeeperKafkaProviderRepository zkConnProviderRepository,
                                       LocationUriParser locationUriParser) {
        this.zkConnProviderRepository = zkConnProviderRepository;
        this.vertx = vertx;
        this.locationUriParser = locationUriParser;
    }

    @Override
    public Future<MppwKafkaRequest> create(EdmlRequestContext context) {
        return Future.future(promise -> {
            LocationUriParser.KafkaTopicUri kafkaTopicUri =
                    locationUriParser.parseKafkaLocationPath(context.getSourceEntity().getExternalTableLocationPath());
            getBrokers(kafkaTopicUri.getAddress())
                    .map(brokers ->
                            MppwKafkaRequest.builder()
                                    .requestId(context.getRequest().getQueryRequest().getRequestId())
                                    .envName(context.getEnvName())
                                    .datamartMnemonic(context.getRequest().getQueryRequest().getDatamartMnemonic())
                                    .isLoadStart(true)
                                    .sysCn(context.getSysCn())
                                    .destinationTableName(context.getDestinationEntity().getName())
                                    .sourceEntity(context.getSourceEntity())
                                    .brokers(brokers)
                                    .topic(kafkaTopicUri.getTopic())
                                    .primaryKeys(context.getDestinationEntity().getFields().stream()
                                            .filter(field -> field.getPrimaryOrder() != null)
                                            .sorted(Comparator.comparingInt(EntityField::getPrimaryOrder))
                                            .map(EntityField::getName)
                                            .collect(Collectors.toList()))
                                    .uploadMetadata(UploadExternalEntityMetadata.builder()
                                            .name(context.getSourceEntity().getName())
                                            .format(context.getSourceEntity().getExternalTableFormat())
                                            .locationPath(context.getSourceEntity().getExternalTableLocationPath())
                                            .externalSchema(context.getSourceEntity().getExternalTableSchema())
                                            .uploadMessageLimit(context.getSourceEntity().getExternalTableUploadMessageLimit())
                                            .build())
                                    .build())
                    .onComplete(promise);
        });
    }

    private Future<List<KafkaBrokerInfo>> getBrokers(String connectionString) {
        return Future.future(promise -> this.vertx.executeBlocking(blockingPromise -> {
            try {
                blockingPromise.complete(zkConnProviderRepository.getOrCreate(connectionString).getKafkaBrokers());
            } catch (Exception e) {
                blockingPromise.fail(new UnreachableLocationException(connectionString, e));
            }
        }, false, promise));
    }
}
