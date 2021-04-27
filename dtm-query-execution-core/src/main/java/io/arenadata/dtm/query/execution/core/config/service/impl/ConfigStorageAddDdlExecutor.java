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
package io.arenadata.dtm.query.execution.core.config.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import io.arenadata.dtm.query.calcite.core.extension.config.function.SqlConfigStorageAdd;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.config.dto.ConfigRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.config.service.ConfigExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.init.service.CoreInitializationService;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ConfigStorageAddDdlExecutor implements ConfigExecutor {

    public static final String STORAGE_IS_NOT_ACTIVE = "Storage [%s] is not active!";
    public static final String SOURCE_TYPE_IS_NOT_SET = "Source type is not set!";
    public static final String CAN_T_ADD_STORAGE = "Can't add storage [%s]";
    private final CalciteDefinitionService calciteDefinitionService;
    private final DataSourcePluginService dataSourcePluginService;
    private final DatamartDao datamartDao;
    private final CoreInitializationService initializationService;

    @Autowired
    public ConfigStorageAddDdlExecutor(@Qualifier("coreCalciteDefinitionService") CalciteDefinitionService calciteDefinitionService,
                                       DataSourcePluginService dataSourcePluginService,
                                       DatamartDao datamartDao, CoreInitializationService initializationService) {
        this.calciteDefinitionService = calciteDefinitionService;
        this.dataSourcePluginService = dataSourcePluginService;
        this.datamartDao = datamartDao;
        this.initializationService = initializationService;
    }

    @Override
    public Future<QueryResult> execute(ConfigRequestContext context) {
        return Future.future(p -> {
            SqlConfigStorageAdd configStorageAdd = (SqlConfigStorageAdd) context.getSqlNode();
            if (configStorageAdd.getSourceType() != null) {
                val sourceType = configStorageAdd.getSourceType();
                if (dataSourcePluginService.getSourceTypes().contains(sourceType)) {
                    datamartDao.getDatamarts()
                            .compose(datamarts -> joinCreateDatamartFutures(sourceType,
                                    datamarts,
                                    context))
                            .compose(v -> initializationService.execute(sourceType))
                            .onSuccess(success -> p.complete(QueryResult.emptyResult()))
                            .onFailure(error -> p.fail(new DtmException(String.format(CAN_T_ADD_STORAGE, sourceType))));
                } else {
                    p.fail(new DtmException(String.format(STORAGE_IS_NOT_ACTIVE, sourceType.name())));
                }
            } else {
                p.fail(new DtmException(SOURCE_TYPE_IS_NOT_SET));
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

    private Future<Void> createDatamartFuture(SourceType sourceType,
                                              String schemaName,
                                              ConfigRequestContext context) {
        return Future.future(p -> {
            DdlRequestContext ddlRequestContext = createDdlRequestContext(schemaName, sourceType, context);
            dataSourcePluginService.ddl(sourceType, context.getMetrics(), DdlRequest.builder()
                    .requestId(context.getRequest().getQueryRequest().getRequestId())
                    .datamartMnemonic(ddlRequestContext.getDatamartName())
                    .envName(context.getEnvName())
                    .sqlKind(SqlKind.OTHER_DDL)
                    .build())
                    .onComplete(p);
        });
    }

    @SneakyThrows
    private DdlRequestContext createDdlRequestContext(String schemaName, SourceType sourceType, ConfigRequestContext context) {
        val createDataBaseQuery = String.format("CREATE DATABASE IF NOT EXISTS %s", schemaName);
        val createDataBaseQueryNode = calciteDefinitionService.processingQuery(createDataBaseQuery);
        val ddlRequest = new DatamartRequest(QueryRequest.builder()
                .datamartMnemonic(schemaName)
                .sql(createDataBaseQuery)
                .build());
        return new DdlRequestContext(context.getMetrics(),
                ddlRequest,
                createDataBaseQueryNode,
                sourceType,
                context.getEnvName());
    }

    @Override
    public SqlConfigType getConfigType() {
        return SqlConfigType.CONFIG_STORAGE_ADD;
    }

}
