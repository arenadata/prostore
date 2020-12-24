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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adqm.common.DdlUtils;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.StatusReportDto;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmRestMppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.StatusReporter;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.*;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.*;
import static io.arenadata.dtm.query.execution.plugin.adqm.common.DdlUtils.avroTypeToNative;
import static io.arenadata.dtm.query.execution.plugin.adqm.common.DdlUtils.splitQualifiedTableName;
import static io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.LoadType.KAFKA;
import static io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.LoadType.REST;
import static java.lang.String.format;

@Component("adqmMppwStartRequestHandler")
@Slf4j
public class MppwStartRequestHandler implements MppwRequestHandler {
    private static final String QUERY_TABLE_SETTINGS = "select %s from system.tables where database = '%s' and name = '%s'";
    private static final String BUFFER_SHARD_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (%s, sys_op_buffer Nullable(Int8)) ENGINE = Join(ANY, INNER, %s)";
    private static final String BUFFER_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s AS %s ENGINE=%s";
    private static final String BUFFER_LOADER_TEMPLATE = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s ON CLUSTER %s TO %s\n" +
            "  AS SELECT %s FROM %s";
    private static final String ACTUAL_LOADER_TEMPLATE = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s ON CLUSTER %s TO %s\n" +
            "AS SELECT %s, %d AS sys_from, 9223372036854775807 as sys_to, 0 as sys_op_load, '9999-12-31 00:00:00' as close_date, 1 AS sign " +
            " FROM %s es WHERE es.sys_op <> 1";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;
    private final MppwProperties mppwProperties;
    private final StatusReporter statusReporter;
    private final Map<LoadType, ExtTableCreator> extTableCreators = new HashMap<>();
    private final RestLoadClient restLoadClient;
    private final AdqmRestMppwKafkaRequestFactory restMppwKafkaRequestFactory;

    @Autowired
    public MppwStartRequestHandler(DatabaseExecutor databaseExecutor,
                                   DdlProperties ddlProperties,
                                   AppConfiguration appConfiguration,
                                   MppwProperties mppwProperties,
                                   StatusReporter statusReporter,
                                   RestLoadClient restLoadClient,
                                   AdqmRestMppwKafkaRequestFactory restMppwKafkaRequestFactory) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
        this.mppwProperties = mppwProperties;
        this.statusReporter = statusReporter;
        this.restLoadClient = restLoadClient;
        this.restMppwKafkaRequestFactory = restMppwKafkaRequestFactory;

        extTableCreators.put(KAFKA, new KafkaExtTableCreator(ddlProperties, mppwProperties));
        extTableCreators.put(REST, new RestExtTableCreator(ddlProperties));
    }

    @Override
    public Future<QueryResult> execute(MppwRequest request) {
        MppwExtTableContext mppwExtTableCtx = new MppwExtTableContext();

        val err = DdlUtils.validateRequest(request);
        if (err.isPresent()) {
            return Future.failedFuture(err.get());
        }
        try {
            mppwExtTableCtx.setSchema(new Schema.Parser()
                    .parse(request.getKafkaParameter().getUploadMetadata().getExternalSchema()));
        } catch (Exception e) {
            return Future.failedFuture(e);
        }

        mppwExtTableCtx.setFullName(DdlUtils.getQualifiedTableName(request, appConfiguration));
        reportStart(request.getKafkaParameter().getTopic(), mppwExtTableCtx.getFullName());

        // 1. Determine table engine (_actual_shard)
        return getTableSetting(mppwExtTableCtx.getFullName() + ACTUAL_POSTFIX,
                "engine_full",
                createVarcharColumnMetadata("engine_full"))
                .map(engineFull -> {
                    mppwExtTableCtx.setEngineFull(engineFull);
                    return engineFull;
                })
                .compose(engineFull ->
                        //2. Get sorting order (_actual)
                        getTableSetting(mppwExtTableCtx.getFullName() + ACTUAL_SHARD_POSTFIX,
                                "sorting_key",
                                createVarcharColumnMetadata("sorting_key")))
                .map(keys -> {
                    mppwExtTableCtx.setSortingKeys(keys);
                    return keys;
                })
                .compose(k ->
                        // 3. Create _ext_shard based on schema from request
                        createExternalTable(request.getKafkaParameter().getTopic(),
                                mppwExtTableCtx.getFullName(),
                                mppwExtTableCtx.getSchema(),
                                mppwExtTableCtx.getSortingKeys())
                )
                .compose(v ->
                        // 4. Create _buffer_shard
                        createBufferShardTable(mppwExtTableCtx.getFullName() + BUFFER_SHARD_POSTFIX,
                                mppwExtTableCtx.getSortingKeys(),
                                mppwExtTableCtx.getSchema())
                )
                .compose(v ->
                        // 5. Create _buffer
                        createBufferTable(mppwExtTableCtx.getFullName() + BUFFER_POSTFIX,
                                mppwExtTableCtx.getEngineFull())
                )
                .compose(v ->
                        // 6. Create _buffer_loader_shard
                        createBufferLoaderTable(mppwExtTableCtx.getFullName() + BUFFER_LOADER_SHARD_POSTFIX,
                                mppwExtTableCtx.getSortingKeys())
                )
                .compose(v ->
                        // 7. Create _actual_loader_shard
                        createActualLoaderTable(mppwExtTableCtx.getFullName() + ACTUAL_LOADER_SHARD_POSTFIX,
                                mppwExtTableCtx.getSchema(),
                                request.getKafkaParameter().getSysCn())
                )
                .compose(v -> createRestInitiator(request))
                .map(v -> QueryResult.emptyResult())
                .onSuccess(Future::succeededFuture)
                .onFailure(fail -> {
                    reportError(request.getKafkaParameter().getTopic());
                    Future.failedFuture(fail);
                });
    }

    private List<ColumnMetadata> createVarcharColumnMetadata(String column) {
        List<ColumnMetadata> metadata = new ArrayList<>();
        metadata.add(new ColumnMetadata(column, ColumnType.VARCHAR));
        return metadata;
    }

    private Future<String> getTableSetting(@NonNull String table, @NonNull String settingKey, List<ColumnMetadata> metadata) {
        val nameParts = splitQualifiedTableName(table);
        if (!nameParts.isPresent()) {
            return Future.failedFuture(format("Cannot parse table name %s", table));
        }
        Promise<String> result = Promise.promise();
        String query = format(QUERY_TABLE_SETTINGS, settingKey, nameParts.get().getLeft(), nameParts.get().getRight());
        databaseExecutor.execute(query, metadata, ar -> {
            if (ar.failed()) {
                result.fail(ar.cause());
                return;
            }
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rows = ar.result();
            if (rows.isEmpty()) {
                result.fail(format("Cannot find %s for %s", settingKey, table));
                return;
            }

            result.complete(rows.get(0).get(settingKey).toString());
        });
        return result.future();
    }

    @NonNull
    private String getConsumerGroupName(@NonNull String tableName) {
        return mppwProperties.getLoadType() == KAFKA ?
                mppwProperties.getConsumerGroup() + tableName :
                mppwProperties.getRestLoadConsumerGroup();
    }

    private Future<Void> createExternalTable(@NonNull String topic,
                                             @NonNull String table,
                                             @NonNull Schema schema,
                                             @NonNull String sortingKey) {
        LoadType loadType = mppwProperties.getLoadType();
        ExtTableCreator creator = extTableCreators.get(loadType);
        String query = creator.generate(topic, table, schema, sortingKey);
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createBufferShardTable(@NonNull String tableName,
                                                @NonNull String columns,
                                                @NonNull Schema schema) {
        String[] cols = columns.split(",\\s*");
        String colString = Arrays.stream(cols)
                .filter(c -> !c.equalsIgnoreCase(SYS_FROM_FIELD))
                .map(c -> format("%s %s", c, findTypeForColumn(c, schema)))
                .collect(Collectors.joining(", "));

        String joinString = Arrays.stream(cols)
                .filter(c -> !c.equalsIgnoreCase(SYS_FROM_FIELD))
                .collect(Collectors.joining(", "));

        String query = format(BUFFER_SHARD_TEMPLATE, tableName, ddlProperties.getCluster(), colString,
                joinString);
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createBufferTable(@NonNull String tableName, @NonNull String engine) {
        String query = format(BUFFER_TEMPLATE, tableName, ddlProperties.getCluster(),
                tableName.replaceAll(BUFFER_POSTFIX, BUFFER_SHARD_POSTFIX),
                engine.replaceAll(ACTUAL_SHARD_POSTFIX, BUFFER_SHARD_POSTFIX));
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createBufferLoaderTable(@NonNull String table, @NonNull String columns) {
        String query = format(BUFFER_LOADER_TEMPLATE, table, ddlProperties.getCluster(),
                table.replaceAll(BUFFER_LOADER_SHARD_POSTFIX, BUFFER_POSTFIX),
                columns.replaceAll(SYS_FROM_FIELD, SYS_OP_FIELD + " AS sys_op_buffer"),
                table.replaceAll(BUFFER_LOADER_SHARD_POSTFIX, EXT_SHARD_POSTFIX));
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createActualLoaderTable(@NonNull String table,
                                                 @NonNull Schema schema,
                                                 long deltaHot) {
        String columns = schema.getFields().stream().map(Schema.Field::name)
                .filter(c -> !c.equalsIgnoreCase(SYS_OP_FIELD))
                .map(c -> "es." + c)
                .collect(Collectors.joining(", "));

        String query = format(ACTUAL_LOADER_TEMPLATE, table, ddlProperties.getCluster(),
                table.replaceAll(ACTUAL_LOADER_SHARD_POSTFIX, ACTUAL_POSTFIX),
                columns, deltaHot,
                table.replaceAll(ACTUAL_LOADER_SHARD_POSTFIX, EXT_SHARD_POSTFIX));
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createRestInitiator(MppwRequest mppwRequest) {
        LoadType loadType = mppwProperties.getLoadType();
        //it means that if we use KAFKA instead of REST load type of mppw, we shouldn't send rest request
        if (loadType == KAFKA) {
            return Future.succeededFuture();
        }
        try {
            final RestMppwKafkaLoadRequest mppwKafkaLoadRequest = restMppwKafkaRequestFactory.create(mppwRequest);
            log.debug("ADQM: Send mppw kafka starting rest request {}", mppwKafkaLoadRequest);
            return restLoadClient.initiateLoading(mppwKafkaLoadRequest);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    private String findTypeForColumn(@NonNull String columnName, @NonNull Schema schema) {
        // Sub-optimal find via full scan of schema
        val field = schema.getFields().stream().filter(a -> a.name().equalsIgnoreCase(columnName)).findFirst();
        return field.map(f -> avroTypeToNative(f.schema())).orElse("Int64");
    }

    private void reportStart(String topic, String fullName) {
        StatusReportDto start = new StatusReportDto(topic, getConsumerGroupName(fullName));
        statusReporter.onStart(start);
    }

    private void reportFinish(String topic) {
        StatusReportDto start = new StatusReportDto(topic);
        statusReporter.onFinish(start);
    }

    private void reportError(String topic) {
        StatusReportDto start = new StatusReportDto(topic);
        statusReporter.onError(start);
    }

    @Data
    @NoArgsConstructor
    private static class MppwExtTableContext {
        private String fullName;
        private String engineFull;
        private String sortingKeys;
        private Schema schema;
    }
}
