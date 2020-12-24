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

import io.arenadata.dtm.common.model.ddl.*;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class MetadataSqlFactoryImpl implements MetadataSqlFactory {
    /**
     * Name of the table of actual data
     */
    public static final String ACTUAL_TABLE = "actual";
    /**
     * History table name
     */
    public static final String HISTORY_TABLE = "history";
    /**
     * Name staging table
     */
    public static final String STAGING_TABLE = "staging";
    /**
     * Delta Number System Field
     */
    public static final String SYS_FROM_ATTR = "sys_from";
    /**
     * System field of maximum delta number
     */
    public static final String SYS_TO_ATTR = "sys_to";
    /**
     * System field of operation on an object
     */
    public static final String SYS_OP_ATTR = "sys_op";
    /**
     * Prefix of writable external table
     */
    public static final String WRITABLE_EXT_TABLE_PREF = "FDW_EXT_";
    public static final String COMMIT_OFFSETS = "SELECT kadb.commit_offsets('%s.%s'::regclass::oid)";
    public static final String SERVER_NAME_TEMPLATE = "FDW_KAFKA_%s";
    public static final String QUERY_DELIMITER = "; ";
    public static final String TABLE_POSTFIX_DELIMITER = "_";
    public static final String WRITABLE_EXTERNAL_TABLE_PREF = "PXF_EXT_";

    private static final String DELIMITER = ", ";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS ";
    private static final String DROP_SCHEMA = "DROP SCHEMA IF EXISTS %s CASCADE";
    private static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s";
    private static final String KEY_COLUMNS_TEMPLATE_SQL = "SELECT c.column_name, c.data_type\n" +
            "FROM information_schema.table_constraints tc\n" +
            "         JOIN information_schema.KEY_COLUMN_USAGE AS ccu USING (constraint_schema, constraint_name)\n" +
            "         JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema\n" +
            "    AND tc.table_name = c.table_name AND ccu.column_name = c.column_name\n" +
            "WHERE constraint_type = 'PRIMARY KEY'\n" +
            "  and c.table_schema = '%s' and tc.table_name = '%s'";
    private static final String CREATE_INDEX_SQL = "CREATE INDEX %s_%s_%s ON %s.%s_%s (%s)";
    private static final String CREAT_WRITABLE_EXT_TABLE_SQL = "CREATE WRITABLE EXTERNAL TABLE %s.%s ( %s )\n" +
            "    LOCATION ('pxf://%s?PROFILE=kafka&BOOTSTRAP_SERVERS=%s&BATCH_SIZE=%d')\n" +
            "    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')";
    public static final String INSERT_INTO_WRITABLE_EXT_TABLE_SQL = "INSERT INTO %s.%s %s";
    public static final String DROP_WRITABLE_EXT_TABLE_SQL = "DROP EXTERNAL TABLE IF EXISTS %s.%s";
    private static final String DROP_FOREIGN_TABLE_SQL = "DROP FOREIGN TABLE IF EXISTS %s.%s";
    private static final String CREATE_FOREIGN_TABLE_SQL =
            "CREATE FOREIGN TABLE %s.%s (%s)\n" +
                    "SERVER %s\n" +
                    "OPTIONS (\n" +
                    "    format '%s',\n" +
                    "    k_topic '%s',\n" +
                    "    k_consumer_group '%s',\n" +
                    "    k_seg_batch '%s',\n" +
                    "    k_timeout_ms '%s',\n" +
                    "    k_initial_offset '0'\n" +
                    ")";
    private static final String CHECK_SERVER_SQL = "select fs.foreign_server_name from information_schema.foreign_servers fs\n" +
                    "join information_schema.foreign_server_options fso\n" +
                    "on fs.foreign_server_catalog = fso.foreign_server_catalog\n" +
                    "and fs.foreign_server_name = fso.foreign_server_name\n" +
                    "where fs.foreign_server_catalog = '%s'\n" +
                    "and fs.foreign_data_wrapper_catalog = '%s'\n" +
                    "and fs.foreign_data_wrapper_name = 'kadb_fdw'\n" +
                    "and fso.option_name = 'k_brokers'\n" +
                    "and fso.option_value = '%s'\n" +
                    "LIMIT 1";
    private static final String CREATE_SERVER_SQL =
            "CREATE SERVER FDW_KAFKA_%s\n" +
                    "FOREIGN DATA WRAPPER kadb_fdw\n" +
                    "OPTIONS (\n" +
                    "  k_brokers '%s'\n" +
                    ")";
    private final static String INSERT_INTO_STAGING_TABLE_SQL = "INSERT INTO %s.%s (%s) SELECT %s FROM %s.%s";

    @Override
    public String createDropTableScript(Entity entity) {
        return new StringBuilder()
                .append(DROP_TABLE).append(entity.getNameWithSchema())
                .append(TABLE_POSTFIX_DELIMITER).append(ACTUAL_TABLE)
                .append(QUERY_DELIMITER)
                .append(DROP_TABLE).append(entity.getNameWithSchema())
                .append(TABLE_POSTFIX_DELIMITER).append(HISTORY_TABLE)
                .append(QUERY_DELIMITER)
                .append(DROP_TABLE).append(entity.getNameWithSchema())
                .append(TABLE_POSTFIX_DELIMITER).append(STAGING_TABLE)
                .append(QUERY_DELIMITER)
                .toString();
    }

    @Override
    public String createSchemaSqlQuery(String schema) {
        return String.format(CREATE_SCHEMA, schema);
    }

    @Override
    public String dropSchemaSqlQuery(String schema) {
        return String.format(DROP_SCHEMA, schema);
    }

    @Override
    public String createKeyColumnsSqlQuery(String schema, String table) {
        return String.format(KEY_COLUMNS_TEMPLATE_SQL, schema, table + TABLE_POSTFIX_DELIMITER + ACTUAL_TABLE);
    }

    @Override
    public String createSecondaryIndexSqlQuery(String schema, String table) {
        StringBuilder sb = new StringBuilder();
        final String idxPostfix = "_idx";
        sb.append(String.format(CREATE_INDEX_SQL, table, ACTUAL_TABLE,
                SYS_FROM_ATTR + idxPostfix, schema, table, ACTUAL_TABLE,
                String.join(DELIMITER, Collections.singletonList(SYS_FROM_ATTR))));
        sb.append(QUERY_DELIMITER);
        sb.append(String.format(CREATE_INDEX_SQL, table, HISTORY_TABLE,
                SYS_TO_ATTR + idxPostfix, schema, table, HISTORY_TABLE,
                String.join(DELIMITER, Arrays.asList(SYS_TO_ATTR, SYS_OP_ATTR))));
        return sb.toString();
    }

    @Override
    public List<ColumnMetadata> createKeyColumnQueryMetadata() {
        List<ColumnMetadata> metadata = new ArrayList<>();
        metadata.add(new ColumnMetadata("column_name", ColumnType.VARCHAR));
        metadata.add(new ColumnMetadata("data_type", ColumnType.VARCHAR));
        return metadata;
    }

    @Override
    public String dropExtTableSqlQuery(String schema, String table) {
        return String.format(DROP_FOREIGN_TABLE_SQL, schema, table);
    }

    @Override
    public String createExtTableSqlQuery(String server, List<String> columnNameTypeList, MppwRequestContext context, MppwProperties mppwProperties) {
        val schema = context.getRequest().getKafkaParameter().getDatamart();
        val table = MetadataSqlFactoryImpl.WRITABLE_EXT_TABLE_PREF + context.getRequest().getQueryRequest().getRequestId().toString().replaceAll("-", "_");
        val columns = String.join(DELIMITER, columnNameTypeList);
        val format = context.getRequest().getKafkaParameter().getUploadMetadata().getFormat().getName();
        val topic = context.getRequest().getKafkaParameter().getTopic();
        val consumerGroup = mppwProperties.getConsumerGroup();
        val uploadMessageLimit = ((UploadExternalEntityMetadata) context.getRequest().getKafkaParameter().getUploadMetadata()).getUploadMessageLimit();
        val chunkSize = uploadMessageLimit != null ? uploadMessageLimit : mppwProperties.getDefaultMessageLimit();
        val timeout = mppwProperties.getFdwTimeoutMs();
        return String.format(CREATE_FOREIGN_TABLE_SQL, schema, table, columns, server, format, topic, consumerGroup, chunkSize, timeout);
    }

    @Override
    public String checkServerSqlQuery(String database, String brokerList) {
        return String.format(CHECK_SERVER_SQL, database, database, brokerList);
    }

    @Override
    public String createServerSqlQuery(String database, String brokerList) {
        return String.format(CREATE_SERVER_SQL, database, brokerList);
    }

    @Override
    public String insertIntoStagingTableSqlQuery(String schema, String columns, String table, String extTable) {
        val stagingTable = new StringBuilder()
                .append(table)
                .append(TABLE_POSTFIX_DELIMITER)
                .append(STAGING_TABLE)
                .toString();
        return String.format(INSERT_INTO_STAGING_TABLE_SQL, schema, stagingTable, columns, columns, schema, extTable);
    }
    private String getColumnDDLByField(EntityField field) {
        val sb = new StringBuilder();
        sb.append(field.getName())
                .append(" ")
                .append(EntityTypeUtil.pgFromDtmType(field))
                .append(" ");
        if (!field.getNullable()) {
            sb.append("NOT NULL");
        }
        return sb.toString();
    }

    @Override
    public String createWritableExtTableSqlQuery(MpprRequest request) {
        val schema = request.getQueryRequest().getDatamartMnemonic();
        val table = MetadataSqlFactoryImpl.WRITABLE_EXTERNAL_TABLE_PREF + request.getQueryRequest().getRequestId().toString().replaceAll("-", "_");
        val columns = request.getDestinationEntity().getFields().stream()
                .map(field -> field.getName() + " " + EntityTypeUtil.pgFromDtmType(field)).collect(Collectors.toList());
        val topic = request.getKafkaParameter().getTopic();
        val brokers = request.getKafkaParameter().getBrokers().stream()
                .map(kafkaBrokerInfo -> kafkaBrokerInfo.getAddress()).collect(Collectors.toList());
        val chunkSize = ((DownloadExternalEntityMetadata) request.getKafkaParameter().getDownloadMetadata()).getChunkSize();
        return String.format(CREAT_WRITABLE_EXT_TABLE_SQL, schema, table, String.join(DELIMITER, columns), topic, String.join(DELIMITER, brokers), chunkSize);
    }

    @Override
    public String insertIntoWritableExtTableSqlQuery(String schema, String table, String enrichedSql) {
        return String.format(INSERT_INTO_WRITABLE_EXT_TABLE_SQL, schema, table, enrichedSql);
    }

    @Override
    public String dropWritableExtTableSqlQuery(String schema, String table) {
        return String.format(DROP_WRITABLE_EXT_TABLE_SQL, schema, table);
    }

    @Override
    public List<String> getColumnsFromEntity(Entity entity) {
        return entity.getFields().stream()
                .map(this::getColumnDDLByField)
                .collect(Collectors.toList());
    }
}
