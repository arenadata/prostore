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
package io.arenadata.dtm.query.execution.core.util;

public class QueryUtil {

    public static final String CREATE_DB = "CREATE DATABASE %s;";
    public static final String DROP_DB = "DROP DATABASE %s;";
    public static final String DROP_TABLE = "DROP TABLE %s.%s;";
    public static final String SELECT_DATAMART_INFO = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE schema_name = '%s';";
    public static final String SELECT_TABLE_INFO = "SELECT table_catalog, table_schema, table_name, table_type\n" +
            "FROM information_schema.tables \n" +
            "WHERE table_schema = '%s' and table_name = '%s';";
    public static final String DROP_UPLOAD_EXT_TABLE = "DROP UPLOAD EXTERNAL TABLE %s.%s;";
    public static final String DROP_DOWNLOAD_EXT_TABLE = "DROP DOWNLOAD EXTERNAL TABLE %s.%s;";
    public static final String INSERT_QUERY = "INSERT INTO %s.%s select %s from %s.%s";
    public static final String GET_DELTA_BY_NUMBER = "GET_DELTA_BY_NUM(%d)";
    public static final String GET_DELTA_BY_DATETIME = "GET_DELTA_BY_DATETIME('%s')";
}