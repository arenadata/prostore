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
package io.arenadata.dtm.query.execution.core.query.client;

import io.vertx.ext.sql.SQLClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SqlClientProviderImpl implements SqlClientProvider {

    private final SqlClientFactory sqlClientFactory;
    private final Map<String, SQLClient> sqlClientMap = new ConcurrentHashMap<>();

    @Autowired
    public SqlClientProviderImpl(SqlClientFactory sqlClientFactory) {
        this.sqlClientFactory = sqlClientFactory;
    }

    @Override
    public SQLClient get(String datamart) {
        final SQLClient sqlClient = sqlClientMap.get(datamart);
        if (sqlClient == null) {
            final SQLClient newSqlClient = sqlClientFactory.create(datamart);
            sqlClientMap.put(datamart, newSqlClient);
            return newSqlClient;
        } else {
            return sqlClient;
        }
    }
}
