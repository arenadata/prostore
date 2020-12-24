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
package io.arenadata.dtm.query.execution.plugin.adqm.configuration;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.ClickhouseProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.datasource.AdqmBalancedClickhouseDataSource;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.AdqmQueryExecutor;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;

@Configuration
public class QueryConfiguration {

    @Bean("adqmQueryExecutor")
    public AdqmQueryExecutor clickhouse(@Qualifier("coreVertx") Vertx vertx,
                                        ClickhouseProperties clickhouseProperties,
                                        @Qualifier("adqmTypeToSqlTypeConverter") SqlTypeConverter typeConverter) {
        String url = String.format("jdbc:clickhouse://%s/%s", clickhouseProperties.getHosts(),
            clickhouseProperties.getDatabase());
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(clickhouseProperties.getUser());
        properties.setPassword(clickhouseProperties.getPassword());
        properties.setSocketTimeout(clickhouseProperties.getSocketTimeout());
        properties.setDataTransferTimeout(clickhouseProperties.getDataTransferTimeout());
        DataSource dataSource = new AdqmBalancedClickhouseDataSource(url, properties);
        return new AdqmQueryExecutor(vertx, dataSource, typeConverter);
    }
}
