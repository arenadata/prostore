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
package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

import java.util.List;
import java.util.UUID;

public interface KafkaMppwSqlFactory extends MppwSqlFactory {

    String moveOffsetsExtTableSqlQuery(String schema, String table);

    String commitOffsetsSqlQuery(String schema, String table);

    String insertIntoKadbOffsetsSqlQuery(String schema, String table);

    String createExtTableSqlQuery(String server, List<String> columnNameTypeList, MppwKafkaRequest request,
                                  MppwProperties mppwProperties);

    String checkServerSqlQuery(String database, String brokerList);

    String createServerSqlQuery(String database, UUID requestId, String brokerList);

    List<String> getColumnsFromEntity(Entity entity);

    String dropExtTableSqlQuery(String schema, String table);

    String insertIntoStagingTableSqlQuery(String schema, String columns, String table, String extTable);

    String getTableName(String requestId);

    String getServerName(String database, UUID requestId);
}
