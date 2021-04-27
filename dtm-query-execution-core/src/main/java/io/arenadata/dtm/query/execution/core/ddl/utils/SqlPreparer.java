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
package io.arenadata.dtm.query.execution.core.ddl.utils;

import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.common.exception.DtmException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.avatica.util.Quoting;
import org.springframework.util.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SqlPreparer {

    public static final String UNABLE_TO_GET_VIEW_NAME = "Unable to get view name";
    public static final String VIEW_NAME_PATH = "_VIEW.IDENTIFIER";
    private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile("(?<=\\stable\\s)([A-z.0-9\"]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_DISTRIBUTED_TABLE_PATTERN = Pattern.compile("(DISTRIBUTED BY.+$)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_TABLE_EXISTS_PATTERN = Pattern.compile("(?<=\\stable if not exists\\s)([A-z.0-9\"]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHECK_CREATE_OR_REPLACE_PATTERN = Pattern.compile("(?i)^(\\s+)?CREATE\\s+OR\\s+REPLACE");
    private static final Pattern CHECK_ALTER_PATTERN = Pattern.compile("(?i)^(\\s+)?ALTER");
    private static final Pattern GET_VIEW_QUERY_PATTERN = Pattern.compile("(?i)view\\s+\\w+\\s+as\\s+(SELECT.*)");
    private static final String SERVICE_DB_NAME = "dtmservice";

    /**
     * We define a schema and a table where we will create a physical "dummy".
     *
     * @param targetSchema Schema
     * @param table        table name
     * @return table with correct schema
     */
    public static String getTableWithSchema(String targetSchema, String table) {
        int indexComma = table.indexOf(".");
        if (indexComma != -1) {
            String schema = StringUtils.isEmpty(targetSchema) ? table.substring(0, indexComma) : targetSchema;
            String name = table.substring(indexComma + 1);
            return schema + "." + name;
        } else {
            String schema = StringUtils.isEmpty(targetSchema) ? SERVICE_DB_NAME : targetSchema;
            return schema + "." + table;
        }
    }

    /**
     * Replaces the table name in the query if the table came without a schema
     *
     * @param sql             Query
     * @param tableWithSchema schematic table
     * @return enriched query
     */
    public static String replaceTableInSql(String sql, String tableWithSchema) {
        if (sql.toLowerCase().contains(tableWithSchema.toLowerCase())) {
            return sql;
        }
        Matcher matcher = CREATE_TABLE_EXISTS_PATTERN.matcher(sql);
        if (matcher.find()) {
            return matcher.replaceAll(tableWithSchema);
        }
        matcher = CREATE_TABLE_PATTERN.matcher(sql);
        if (matcher.find()) {
            return matcher.replaceAll(tableWithSchema);
        }
        return sql;
    }

    /**
     * Replace double quotes with back ones, because such are used in Mary
     *
     * @param sql query
     * @return query with correct quotes for mary
     */
    public static String replaceQuote(String sql) {
        return sql.replace(Quoting.DOUBLE_QUOTE.string, Quoting.BACK_TICK.string);
    }

    public static String removeDistributeBy(String sql) {
        Matcher matcher = CREATE_DISTRIBUTED_TABLE_PATTERN.matcher(sql);
        if (matcher.find()) {
            return matcher.replaceFirst("");
        }
        return sql;
    }

    public static SqlTreeNode getViewNameNode(SqlSelectTree tree) {
        val namesByView = tree.findNodesByPath(VIEW_NAME_PATH);
        if (namesByView.isEmpty()) {
            throw new DtmException(UNABLE_TO_GET_VIEW_NAME);
        } else {
            return namesByView.get(0);
        }
    }

    public static boolean isCreateOrReplace(String sql) {
        return CHECK_CREATE_OR_REPLACE_PATTERN.matcher(sql).find();
    }

    public static boolean isAlter(String sql) {
        return CHECK_ALTER_PATTERN.matcher(sql).find();
    }
}
