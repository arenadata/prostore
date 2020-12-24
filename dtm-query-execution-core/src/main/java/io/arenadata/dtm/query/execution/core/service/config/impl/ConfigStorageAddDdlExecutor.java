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

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import io.arenadata.dtm.query.calcite.core.extension.config.function.SqlConfigStorageAdd;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.config.ConfigExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ConfigStorageAddDdlExecutor implements ConfigExecutor {

    public static final String STORAGE_S_IS_NOT_ACTIVE = "Storage [%s] is not active!";
    public static final String SOURCE_TYPE_IS_NOT_SET = "Source type is not set!";
    public static final String CAN_T_ADD_STORAGE_S = "Can't add storage [%s]";
    private final CalciteDefinitionService calciteDefinitionService;
    private final DataSourcePluginService dataSourcePluginService;
    private final DatamartDao datamartDao;


    @Autowired
    public ConfigStorageAddDdlExecutor(@Qualifier("coreCalciteDefinitionService") CalciteDefinitionService calciteDefinitionService,
                                       DataSourcePluginService dataSourcePluginService,
                                       DatamartDao datamartDao) {
        this.calciteDefinitionService = calciteDefinitionService;
        this.dataSourcePluginService = dataSourcePluginService;
        this.datamartDao = datamartDao;
    }

    @Override
    public Future<QueryResult> execute(ConfigRequestContext context) {
        return Future.future(p -> {
            SqlConfigStorageAdd configStorageAdd = context.getSqlConfigCall();
            if (configStorageAdd.getSourceType() != null) {
                val sourceType = configStorageAdd.getSourceType();
                if (dataSourcePluginService.getSourceTypes().contains(sourceType)) {
                    datamartDao.getDatamarts()
                        .compose(datamarts -> {
                            String envName = context.getRequest().getQueryRequest().getEnvName();
                            return joinCreateDatamartFutures(sourceType, datamarts, context);
                        })
                        .onSuccess(success -> p.complete(QueryResult.emptyResult()))
                        .onFailure(error -> {
                            val errMsg = String.format(CAN_T_ADD_STORAGE_S, sourceType);
                            log.error(errMsg, error);
                            p.fail(errMsg);
                        });
                } else {
                    val errMsg = String.format(STORAGE_S_IS_NOT_ACTIVE, sourceType.name());
                    log.error(errMsg);
                    p.fail(errMsg);
                }
            } else {
                log.error(SOURCE_TYPE_IS_NOT_SET);
                p.fail(SOURCE_TYPE_IS_NOT_SET);
            }
        });
    }

    private CompositeFuture joinCreateDatamartFutures(SourceType sourceType,
                                                      List<String> datamarts,
                                                      ConfigRequestContext context) {
        return CompositeFuture.join(
            datamarts.stream()
                .map(datamart -> createDatamartFuture(sourceType, datamart, context))
                .collect(Collectors.toList()));
    }

    @Override
    public SqlConfigType getConfigType() {
        return SqlConfigType.CONFIG_STORAGE_ADD;
    }

    private Future<Void> createDatamartFuture(SourceType sourceType,
                                              String schemaName,
                                              ConfigRequestContext context) {
        return Future.future(p -> {
            DdlRequestContext ddlRequestContext = getDdlRequestContext(schemaName, context);
            dataSourcePluginService.ddl(sourceType, ddlRequestContext, p);
        });
    }

    @SneakyThrows
    private DdlRequestContext getDdlRequestContext(String schemaName, ConfigRequestContext context) {
        val createDataBaseQuery = "CREATE DATABASE IF NOT EXISTS " + schemaName;
        val createDataBaseQueryNode = calciteDefinitionService.processingQuery(createDataBaseQuery);
        val ddlRequest = new DdlRequest(QueryRequest.builder()
            .envName(context.getRequest().getQueryRequest().getEnvName())
            .datamartMnemonic(schemaName)
            .sql(createDataBaseQuery)
            .build());
        return new DdlRequestContext(context.getMetrics(), ddlRequest, createDataBaseQueryNode);
    }

}
