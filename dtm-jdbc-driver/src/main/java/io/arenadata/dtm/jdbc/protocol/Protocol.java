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
package io.arenadata.dtm.jdbc.protocol;

import io.arenadata.dtm.jdbc.core.QueryResult;
import io.arenadata.dtm.jdbc.model.ColumnInfo;
import io.arenadata.dtm.jdbc.model.SchemaInfo;
import io.arenadata.dtm.jdbc.model.TableInfo;

import java.sql.SQLException;
import java.util.List;

/**
 * Data reader protocol
 */
public interface Protocol {
    /**
     * Get schemas information
     * @return List of schema info
     */
    List<SchemaInfo> getDatabaseSchemas();
    /**
     * Get table information for schema pattern
     * @param schemaPattern - schema pattern
     * @return List of table info
     */
    List<TableInfo> getDatabaseTables(String schemaPattern);
    /**
     * Get column info for schema, table
     * @param schema - schema name
     * @param tableName - table name
     * @return List of column info
     */
    List<ColumnInfo> getDatabaseColumns(String schema, String tableName);
    /**
     * execute sql query without params
     * @param sql - native sql query
     * @return query result
     */
    QueryResult executeQuery(String sql) throws SQLException;
}
