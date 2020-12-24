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
package io.arenadata.dtm.query.execution.core.service.config.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import io.arenadata.dtm.query.execution.plugin.api.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.config.ConfigExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.config.ConfigService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("coreConfigServiceImpl")
public class ConfigServiceImpl implements ConfigService<QueryResult> {

    public static final String NOT_SUPPORTED_CONFIG_QUERY_TYPE = "Not supported config query type";
    private final Map<SqlConfigType, ConfigExecutor> executorMap;

    @Autowired
    public ConfigServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    @Override
    public void execute(ConfigRequestContext context,
                        Handler<AsyncResult<QueryResult>> handler) {
        try {
            SqlConfigCall configCall = context.getSqlConfigCall();
            ConfigExecutor executor = executorMap.get(configCall.getSqlConfigType());
            if (executor != null) {
                executor.execute(context)
                    .onComplete(handler);
            } else {
                log.error(NOT_SUPPORTED_CONFIG_QUERY_TYPE);
                handler.handle(Future.failedFuture(NOT_SUPPORTED_CONFIG_QUERY_TYPE));
            }
        } catch (Exception e) {
            log.error(NOT_SUPPORTED_CONFIG_QUERY_TYPE, e);
            handler.handle(Future.failedFuture(NOT_SUPPORTED_CONFIG_QUERY_TYPE));
        }
    }

    @Override
    public void addExecutor(ConfigExecutor executor) {
        executorMap.put(executor.getConfigType(), executor);
    }

}
