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
package io.arenadata.dtm.query.execution.plugin.adb.mppr.kafka.service;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.mppr.AdbMpprExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.mppr.kafka.factory.KafkaMpprSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MpprDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Slf4j
@Service("adbMpprKafkaService")
public class AdbMpprKafkaService implements AdbMpprExecutor {

    private final QueryEnrichmentService adbQueryEnrichmentService;
    private final KafkaMpprSqlFactory kafkampprSqlFactory;
    private final DatabaseExecutor adbQueryExecutor;

    @Autowired
    public AdbMpprKafkaService(QueryEnrichmentService adbQueryEnrichmentService,
                               KafkaMpprSqlFactory kafkampprSqlFactory,
                               @Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor) {
        this.adbQueryEnrichmentService = adbQueryEnrichmentService;
        this.kafkampprSqlFactory = kafkampprSqlFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        return Future.future(promise -> {
            val mpprKafkaRequest = (MpprKafkaRequest) request;
            val schema = request.getDatamartMnemonic();
            val table = kafkampprSqlFactory.getTableName(request.getRequestId().toString());
            adbQueryExecutor.executeUpdate(kafkampprSqlFactory.createWritableExtTableSqlQuery(mpprKafkaRequest))
                    .compose(v -> enrichQuery(mpprKafkaRequest))
                    .compose(enrichedQuery -> insertIntoWritableExtTableSqlQuery(schema, table, enrichedQuery))
                    .compose(v -> dropWritableExtTableSqlQuery(schema, table))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(err -> dropWritableExtTableSqlQuery(schema, table)
                            .onComplete(dropResult -> {
                                if (dropResult.failed()) {
                                    log.error("Failed to drop writable external table {}.{}", schema, table);
                                }
                                promise.fail(new MpprDatasourceException(
                                        String.format("Failed to unload data from datasource by request %s",
                                                request),
                                        err));
                            }));
        });
    }

    private Future<Void> dropWritableExtTableSqlQuery(String schema, String table) {
        return adbQueryExecutor.executeUpdate(
                kafkampprSqlFactory.dropWritableExtTableSqlQuery(schema,
                        table));
    }

    private Future<Void> insertIntoWritableExtTableSqlQuery(String schema, String table, String sql) {
        return adbQueryExecutor.executeUpdate(
                kafkampprSqlFactory.insertIntoWritableExtTableSqlQuery(schema,
                        table,
                        sql));
    }

    private Future<String> enrichQuery(MpprKafkaRequest request) {
        return adbQueryEnrichmentService.enrich(
                EnrichQueryRequest.builder()
                .query(request.getDmlSubQuery())
                .schema(request.getLogicalSchema())
                .envName(request.getEnvName())
                .deltaInformations(request.getDeltaInformations())
                .build());
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }
}
