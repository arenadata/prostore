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

import io.arenadata.dtm.query.execution.core.AbstractCoreDtmIT;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SqlClientFactoryImpl implements SqlClientFactory {

    private final Vertx vertx;

    @Autowired
    public SqlClientFactoryImpl(@Qualifier("itTestVertx") Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public SQLClient create(String datamartMnemonic) {
        val jdbcUrl = String.format("jdbc:adtm://%s/%s",
                AbstractCoreDtmIT.getDtmCoreHostPortExternal(),
                datamartMnemonic);
        val jsonConfig = new JsonObject()
                .put("driver_class", "io.arenadata.dtm.jdbc.DtmDriver")
                .put("max_pool_size", 5)
                .put("password", "")
                .put("user", "")
                .put("url", jdbcUrl);
        return JDBCClient.create(vertx, jsonConfig);
    }
}
