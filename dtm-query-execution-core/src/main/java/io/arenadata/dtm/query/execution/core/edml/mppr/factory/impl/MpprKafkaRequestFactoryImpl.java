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
package io.arenadata.dtm.query.execution.core.edml.mppr.factory.impl;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.kafka.core.repository.ZookeeperKafkaProviderRepository;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.exception.UnreachableLocationException;
import io.arenadata.dtm.query.execution.core.edml.mppr.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.edml.service.LocationUriParser;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class MpprKafkaRequestFactoryImpl implements MpprKafkaRequestFactory {

    private final ZookeeperKafkaProviderRepository zkConnProviderRepository;
    private final Vertx vertx;
    private final LocationUriParser locationUriParser;

    @Autowired
    public MpprKafkaRequestFactoryImpl(@Qualifier("coreVertx") Vertx vertx,
                                       @Qualifier("mapZkKafkaProviderRepository") ZookeeperKafkaProviderRepository zkConnProviderRepository,
                                       LocationUriParser locationUriParser) {
        this.zkConnProviderRepository = zkConnProviderRepository;
        this.vertx = vertx;
        this.locationUriParser = locationUriParser;
    }

    @Override
    public Future<MpprKafkaRequest> create(EdmlRequestContext context) {
        return Future.future(promise -> {
            LocationUriParser.KafkaTopicUri kafkaTopicUri =
                    locationUriParser.parseKafkaLocationPath(context.getDestinationEntity().getExternalTableLocationPath());
            getBrokers(kafkaTopicUri.getAddress())
                    .map(brokers ->
                            MpprKafkaRequest.builder()
                                    .requestId(context.getRequest().getQueryRequest().getRequestId())
                                    .envName(context.getEnvName())
                                    .datamartMnemonic(context.getRequest().getQueryRequest().getDatamartMnemonic())
                                    .sql(context.getRequest().getQueryRequest().getSql())
                                    .sqlNode(context.getSqlNode())
                                    .brokers(brokers)
                                    .dmlSubQuery(context.getDmlSubQuery())
                                    .destinationEntity(context.getDestinationEntity())
                                    .logicalSchema(context.getLogicalSchema())
                                    .deltaInformations(context.getDeltaInformations())
                                    .topic(kafkaTopicUri.getTopic())
                                    .downloadMetadata(DownloadExternalEntityMetadata.builder()
                                            .name(context.getDestinationEntity().getName())
                                            .format(context.getDestinationEntity().getExternalTableFormat())
                                            .externalSchema(context.getDestinationEntity().getExternalTableSchema())
                                            .locationPath(context.getDestinationEntity().getExternalTableLocationPath())
                                            .chunkSize(context.getDestinationEntity().getExternalTableDownloadChunkSize())
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
