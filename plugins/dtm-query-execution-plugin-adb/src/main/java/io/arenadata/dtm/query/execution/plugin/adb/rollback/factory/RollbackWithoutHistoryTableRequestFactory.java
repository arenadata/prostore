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
package io.arenadata.dtm.query.execution.plugin.adb.rollback.factory;

public class RollbackWithoutHistoryTableRequestFactory extends AdbRollbackRequestFactory {

    private static final String TRUNCATE_STAGING = "TRUNCATE %s.%s_staging";
    private static final String DELETE_FROM_ACTUAL = "DELETE FROM %s.%s_actual WHERE sys_from = %s";
    private static final String INSERT_ACTUAL_SQL = "INSERT INTO %s.%s_actual (%s, sys_from, sys_to, sys_op)\n" +
        "SELECT %s, sys_from, NULL, 0\n" +
        "FROM %s.%s_actual\n" +
        "WHERE sys_to = %s";
    private static final String DELETE_FROM_HISTORY = "DELETE FROM %s.%s_actual WHERE sys_to = %s";

    @Override
    protected String getTruncateStagingSql() {
        return TRUNCATE_STAGING;
    }

    @Override
    protected String getDeleteFromActualSql() {
        return DELETE_FROM_ACTUAL;
    }

    @Override
    protected String getInsertActualSql() {
        return INSERT_ACTUAL_SQL;
    }

    @Override
    protected String getDeleteFromHistorySql() {
        return DELETE_FROM_HISTORY;
    }
}
