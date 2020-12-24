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
package io.arenadata.dtm.query.execution.plugin.adb.configuration;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.DatabaseTypes;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.GreenplumProperties;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

import static io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.DatabaseTypes.GREENPLUM;

@Configuration
public class QueryConfiguration {

  @Bean("adbDatabaseExecutors")
  public Map<DatabaseTypes, DatabaseExecutor> databaseExecutors(AdbQueryExecutor adbQueryExecutor) {
    Map<DatabaseTypes, DatabaseExecutor> beanMap = new HashMap<>();
    beanMap.put(GREENPLUM, adbQueryExecutor);
    return beanMap;
  }


  @Bean("adbQueryExecutor")
  public AdbQueryExecutor greenplam(@Qualifier("coreVertx") Vertx vertx,
                                    GreenplumProperties greenplumProperties,
                                    @Qualifier("adbTypeToSqlTypeConverter") SqlTypeConverter typeConverter) {
    PgPool pgPool = PgClient.pool(greenplumProperties.getOptions());
    return new AdbQueryExecutor(pgPool, greenplumProperties.getFetchSize(), typeConverter);
  }
}
