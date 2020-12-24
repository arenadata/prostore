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
package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

import java.util.List;

/**
 * Factory for creating DDL scripts based on metadata
 */
public interface MetadataSqlFactory {

    String createDropTableScript(Entity entity);

    String createSchemaSqlQuery(String schemaName);

    String dropSchemaSqlQuery(String schemaName);

    String createKeyColumnsSqlQuery(String schema, String tableName);

    String createSecondaryIndexSqlQuery(String schema, String tableName);

    List<ColumnMetadata> createKeyColumnQueryMetadata();

    String createWritableExtTableSqlQuery(MpprRequest request);

    String insertIntoWritableExtTableSqlQuery(String schema, String table, String enrichedSql);

    String dropWritableExtTableSqlQuery(String schema, String table);

    String dropExtTableSqlQuery(String schema, String table);

    String createExtTableSqlQuery(String server, List<String> columnNameTypeList, MppwRequestContext context, MppwProperties mppwProperties);

    String checkServerSqlQuery(String database, String brokerList);

    String createServerSqlQuery(String database, String brokerList);

    String insertIntoStagingTableSqlQuery(String schema, String columns, String table, String extTable);

    List<String> getColumnsFromEntity(Entity entity);
}