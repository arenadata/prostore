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
package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adb.base.utils.AdbTypeUtil;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.ACTUAL_TABLE;
import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.STAGING_TABLE;

@Service("kafkaMppwSqlFactoryImpl")
public class KafkaMppwSqlFactoryImpl implements KafkaMppwSqlFactory {
    private static final String COMMIT_OFFSETS = "SELECT kadb.commit_offsets('%s.%s'::regclass::oid)";
    private static final String SERVER_NAME_TEMPLATE = "FDW_KAFKA_%s";
    private static final String TABLE_POSTFIX_DELIMITER = "_";
    private static final String WRITABLE_EXT_TABLE_PREF = "FDW_EXT_";
    private static final String DELIMITER = ", ";
    private static final String KEY_COLUMNS_TEMPLATE_SQL = "SELECT c.column_name, c.data_type\n" +
            "FROM information_schema.table_constraints tc\n" +
            "         JOIN information_schema.KEY_COLUMN_USAGE AS ccu USING (constraint_schema, constraint_name)\n" +
            "         JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema\n" +
            "    AND tc.table_name = c.table_name AND ccu.column_name = c.column_name\n" +
            "WHERE constraint_type = 'PRIMARY KEY'\n" +
            "  and c.table_schema = '%s' and tc.table_name = '%s'";
    private static final String DROP_FOREIGN_TABLE_SQL = "DROP FOREIGN TABLE IF EXISTS %s.%s";
    private static final String INSERT_INTO_KADB_OFFSETS = "insert into kadb.offsets SELECT * from kadb.load_partitions('%s.%s'::regclass::oid)";
    private static final String MOVE_TO_OFFSETS_FOREIGN_TABLE_SQL = "SELECT kadb.offsets_to_committed('%s.%s'::regclass::oid)";
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
    private static final String INSERT_INTO_STAGING_TABLE_SQL = "INSERT INTO %s.%s (%s) SELECT %s FROM %s.%s";

    @Override
    public String createKeyColumnsSqlQuery(String schema, String table) {
        return String.format(KEY_COLUMNS_TEMPLATE_SQL, schema, table + TABLE_POSTFIX_DELIMITER + ACTUAL_TABLE);
    }

    @Override
    public List<ColumnMetadata> createKeyColumnQueryMetadata() {
        List<ColumnMetadata> metadata = new ArrayList<>();
        metadata.add(new ColumnMetadata("column_name", ColumnType.VARCHAR));
        metadata.add(new ColumnMetadata("data_type", ColumnType.VARCHAR));
        return metadata;
    }

    @Override
    public String moveOffsetsExtTableSqlQuery(String schema, String table) {
        return String.format(MOVE_TO_OFFSETS_FOREIGN_TABLE_SQL, schema, table);
    }

    @Override
    public String commitOffsetsSqlQuery(String schema, String table) {
        return String.format(COMMIT_OFFSETS, schema, table);
    }

    @Override
    public String insertIntoKadbOffsetsSqlQuery(String schema, String table) {
        return String.format(INSERT_INTO_KADB_OFFSETS, schema, table);
    }

    @Override
    public String createExtTableSqlQuery(String server,
                                         List<String> columnNameTypeList,
                                         MppwKafkaRequest request,
                                         MppwProperties mppwProperties) {
        val schema = request.getDatamartMnemonic();
        val table = WRITABLE_EXT_TABLE_PREF +
                request.getRequestId().toString().replace("-", "_");
        val columns = String.join(DELIMITER, columnNameTypeList);
        val format = request.getUploadMetadata().getFormat().getName();
        val topic = request.getTopic();
        val consumerGroup = mppwProperties.getConsumerGroup();
        val uploadMessageLimit = ((UploadExternalEntityMetadata) request.getUploadMetadata()).getUploadMessageLimit();
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
    public List<String> getColumnsFromEntity(Entity entity) {
        return entity.getFields().stream()
                .map(this::getColumnDDLByField)
                .collect(Collectors.toList());
    }

    private String getColumnDDLByField(EntityField field) {
        val sb = new StringBuilder();
        sb.append(field.getName())
                .append(" ")
                .append(AdbTypeUtil.adbTypeFromDtmType(field))
                .append(" ");
        if (!field.getNullable()) {
            sb.append("NOT NULL");
        }
        return sb.toString();
    }

    @Override
    public String dropExtTableSqlQuery(String schema, String table) {
        return String.format(DROP_FOREIGN_TABLE_SQL, schema, table);
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

    @Override
    public String getTableName(String requestId) {
        return WRITABLE_EXT_TABLE_PREF + requestId.replace("-", "_");
    }

    @Override
    public String getServerName(String database) {
        return String.format(SERVER_NAME_TEMPLATE, database);
    }
}
