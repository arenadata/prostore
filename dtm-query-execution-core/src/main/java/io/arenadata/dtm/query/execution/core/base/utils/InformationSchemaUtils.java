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
package io.arenadata.dtm.query.execution.core.base.utils;

public class InformationSchemaUtils {
    public static final String INFORMATION_SCHEMA = "information_schema";
    public static final String LOGIC_SCHEMA_KEY_COLUMN_USAGE =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_key_column_usage AS \n" +
            "SELECT constraint_catalog, constraint_schema, constraint_name, table_schema, table_name, column_name, ordinal_position\n" +
            "FROM information_schema.KEY_COLUMN_USAGE\n" +
            "WHERE constraint_schema NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";
    public static final String LOGIC_SCHEMA_DATAMARTS =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_datamarts AS \n" +
            "SELECT catalog_name, schema_name\n" +
            "FROM information_schema.schemata \n" +
            "WHERE schema_name NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";
    public static final String LOGIC_SCHEMA_ENTITIES =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_entities AS \n" +
            "SELECT table_catalog, table_schema, table_name, table_type\n" +
            "FROM information_schema.tables \n" +
            "WHERE table_schema NOT IN ('DTM', 'INFORMATION_SCHEMА', 'SYSTEM_LOBS')";
    public static final String LOGIC_SCHEMA_COLUMNS =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_columns AS\n" +
                "SELECT c.table_catalog,\n" +
                "       c.table_schema,\n" +
                "       c.table_name,\n" +
                "       c.column_name,\n" +
                "       c.is_nullable,\n" +
                "       c.ordinal_position,\n" +
                "       case c.character_maximum_length" +
                "           when 16777216 then NULL " +
                "           else c.character_maximum_length " +
                "           end as character_maximum_length,\n" +
                "       case when c.datetime_precision != 0 then c.datetime_precision " +
                "       else NULL end as datetime_precision,\n" +
                "       case\n" +
                "           when com.comment is not NULL then comment\n" +
                "           else c.data_type\n" +
                "           end as data_type\n" +
                "FROM information_schema.COLUMNS c\n" +
                "         left outer join information_schema.system_comments com\n" +
                "                         on c.TABLE_CATALOG = com.OBJECT_CATALOG and c.TABLE_SCHEMA = com.OBJECT_SCHEMA and\n" +
                "                            c.TABLE_NAME = com.OBJECT_NAME and\n" +
            "                            c.COLUMN_NAME = com.COLUMN_NAME\n" +
            "WHERE c.table_schema NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";
    public static final String LOGIC_SCHEMA_ENTITY_CONSTRAINTS =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_entity_constraints AS\n" +
            "SELECT kcu.constraint_catalog,\n" +
            "       kcu.constraint_schema,\n" +
            "       si.index_name as constraint_name,\n" +
            "       kcu.table_schema,\n" +
            "       kcu.table_name,\n" +
            "       case\n" +
            "           when si.INDEX_NAME like 'SK_%' then 'sharding key'\n" +
            "           when si.INDEX_NAME like '%_PK_%' then 'primary key'\n" +
            "           ELSE '-'\n" +
            "           end       AS CONSTRAINT_TYPE\n" +
            "FROM information_schema.KEY_COLUMN_USAGE kcu,\n" +
            "     information_schema.SYSTEM_INDEXSTATS si\n" +
            "WHERE kcu.CONSTRAINT_CATALOG = si.TABLE_CATALOG\n" +
            "  and kcu.TABLE_CATALOG = si.TABLE_CATALOG\n" +
            "  and kcu.TABLE_SCHEMA = si.TABLE_SCHEMA\n" +
            "  and kcu.TABLE_NAME = si.TABLE_NAME\n" +
            "  and kcu.constraint_schema NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";
    public static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s";
    public static final String DROP_SCHEMA = "DROP SCHEMA %s CASCADE";

    public static final String CREATE_SHARDING_KEY_INDEX = "CREATE INDEX IF NOT EXISTS sk_%s on %s (%s)";

    public static final String COMMENT_ON_COLUMN = "COMMENT ON COLUMN %s.%s IS '%s'";

    public static final String DROP_VIEW = "DROP VIEW IF EXISTS %s.%s";
    public static final String DROP_TABLE = "DROP TABLE IF EXISTS %s.%s";
    public static final String CHECK_VIEW =
            "SELECT VIEW_NAME\n" +
            "FROM   INFORMATION_SCHEMA.VIEW_TABLE_USAGE\n" +
            "WHERE  TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';";
}
