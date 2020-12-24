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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwKafkaLoadRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Component("adbMppwStartRequestExecutor")
@Slf4j
public class AdbMppwStartRequestExecutorImpl implements AdbMppwRequestExecutor {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory metadataSqlFactory;
    private final MppwTransferRequestFactory mppwTransferRequestFactory;
    private final MppwKafkaLoadRequestFactory mppwKafkaLoadRequestFactory;
    private final Vertx vertx;
    private final MppwProperties mppwProperties;
    private final String dbName;

    @Autowired
    public AdbMppwStartRequestExecutorImpl(AdbQueryExecutor adbQueryExecutor,
                                           MetadataSqlFactory metadataSqlFactory,
                                           MppwTransferRequestFactory mppwTransferRequestFactory,
                                           MppwKafkaLoadRequestFactory mppwKafkaLoadRequestFactory,
                                           @Qualifier("coreVertx") Vertx vertx,
                                           MppwProperties mppwProperties,
                                           @Value("${adb.datasource.options.database}") String dbName) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.metadataSqlFactory = metadataSqlFactory;
        this.mppwTransferRequestFactory = mppwTransferRequestFactory;
        this.mppwKafkaLoadRequestFactory = mppwKafkaLoadRequestFactory;
        this.vertx = vertx;
        this.mppwProperties = mppwProperties;
        this.dbName = dbName;
    }

    @Override
    public Future<QueryResult> execute(MppwRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            val format = context.getRequest().getKafkaParameter().getUploadMetadata().getFormat();
            if (!Format.AVRO.equals(format)) {
                promise.fail(new RuntimeException(String.format("Format %s not implemented", format)));
            }
            List<KafkaBrokerInfo> brokers = context.getRequest().getKafkaParameter().getBrokers();
            getOrCreateServer(brokers, dbName)
                    .compose(server -> createWritableExternalTable(server, context))
                    .compose(server -> createMppwKafkaRequestContext(context, server))
                    .onSuccess(kafkaContext -> {
                        vertx.eventBus().send(MppwTopic.KAFKA_START.getValue(), Json.encode(kafkaContext));
                        log.debug("Mppw started successfully");
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<String> getOrCreateServer(List<KafkaBrokerInfo> brokers, String currentDatabase) {
        return Future.future(promise -> {
            val columnMetadata = Collections.singletonList(new ColumnMetadata("foreign_server_name", ColumnType.VARCHAR));
            val brokersList = brokers.stream().map(KafkaBrokerInfo::getAddress).collect(Collectors.joining(","));
            adbQueryExecutor.execute(metadataSqlFactory.checkServerSqlQuery(currentDatabase, brokersList), columnMetadata, checkServerResult -> {
                if (checkServerResult.succeeded()) {
                    val result = checkServerResult.result();
                    if (result.isEmpty()) {
                        adbQueryExecutor.execute(metadataSqlFactory.createServerSqlQuery(currentDatabase, brokersList), Collections.emptyList(), createServerResult -> {
                            if (createServerResult.succeeded()) {
                                promise.complete(String.format(MetadataSqlFactoryImpl.SERVER_NAME_TEMPLATE, currentDatabase));
                            } else {
                                promise.fail(createServerResult.cause());
                            }
                        });
                    } else {
                        promise.complete(result.get(0).get("foreign_server_name").toString());
                    }
                } else {
                    promise.fail(checkServerResult.cause());
                }
            });
        });
    }

    private Future<String> createWritableExternalTable(String server, MppwRequestContext context) {
        return Future.future(promise -> {
            val sourceEntity = context.getRequest().getKafkaParameter().getSourceEntity();
            val columns = metadataSqlFactory.getColumnsFromEntity(sourceEntity);
            columns.add("sys_op int");
            adbQueryExecutor.executeUpdate(metadataSqlFactory.createExtTableSqlQuery(server, columns, context, mppwProperties), ar -> {
                if (ar.succeeded()) {
                    promise.complete(server);
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private Future<MppwKafkaRequestContext> createMppwKafkaRequestContext(MppwRequestContext context,
                                                                          String server) {
        return Future.future((Promise<MppwKafkaRequestContext> promise) -> {
            final MppwKafkaLoadRequest mppwKafkaLoadRequest = mppwKafkaLoadRequestFactory.create(context, server, mppwProperties);
            final String keyColumnsSqlQuery = metadataSqlFactory.createKeyColumnsSqlQuery(
                    context.getRequest().getKafkaParameter().getDatamart(),
                    context.getRequest().getKafkaParameter().getDestinationTableName());
            final List<ColumnMetadata> metadata = metadataSqlFactory.createKeyColumnQueryMetadata();
            adbQueryExecutor.execute(keyColumnsSqlQuery, metadata, ar -> {
                if (ar.succeeded()) {
                    final MppwTransferDataRequest mppwTransferDataRequest =
                            mppwTransferRequestFactory.create(context, ar.result());
                    MppwKafkaRequestContext kafkaRequestContext =
                            new MppwKafkaRequestContext(mppwKafkaLoadRequest, mppwTransferDataRequest);
                    promise.complete(kafkaRequestContext);
                } else {
                    promise.fail(ar.cause());
                }
            });

        });
    }
}
