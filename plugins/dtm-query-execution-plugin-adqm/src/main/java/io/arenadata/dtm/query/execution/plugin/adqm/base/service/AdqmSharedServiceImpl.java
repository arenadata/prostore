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
package io.arenadata.dtm.query.execution.plugin.adqm.base.service;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.query.execution.plugin.adqm.base.configuration.properties.ClickhouseProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmTablesSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.shared.adqm.AdqmSharedService;
import io.arenadata.dtm.query.execution.plugin.api.shared.adqm.AdqmSharedProperties;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

@Primary
@Service
public class AdqmSharedServiceImpl implements AdqmSharedService {

    private final ClickhouseProperties clickhouseProperties;
    private final DatabaseExecutor databaseExecutor;
    private final AdqmProcessingSqlFactory adqmProcessingSqlFactory;
    private final AdqmTablesSqlFactory adqmTablesSqlFactory;

    public AdqmSharedServiceImpl(ClickhouseProperties clickhouseProperties,
                                 DatabaseExecutor databaseExecutor,
                                 AdqmProcessingSqlFactory adqmProcessingSqlFactory,
                                 AdqmTablesSqlFactory adqmTablesSqlFactory) {
        this.clickhouseProperties = clickhouseProperties;
        this.databaseExecutor = databaseExecutor;
        this.adqmProcessingSqlFactory = adqmProcessingSqlFactory;
        this.adqmTablesSqlFactory = adqmTablesSqlFactory;
    }

    @Override
    public Future<Void> flushActualTable(String env, String datamart, Entity entity) {
        return databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getFlushActualSql(env, datamart, entity.getName()))
                .compose(unused -> databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getOptimizeActualSql(env, datamart, entity.getName())));

    }

    @Override
    public Future<Void> recreateBufferTables(String env, String datamart, Entity entity) {
        val dropBufferQuery = adqmTablesSqlFactory.getDropBufferSql(env, datamart, entity);
        return databaseExecutor.executeUpdate(dropBufferQuery)
                .compose(v -> {
                    val dropBufferShardQuery = adqmTablesSqlFactory.getDropBufferShardSql(env, datamart, entity);
                    return databaseExecutor.executeUpdate(dropBufferShardQuery);
                })
                .compose(v -> {
                    val createBufferShardQuery = adqmTablesSqlFactory.getCreateBufferShardSql(env, datamart, entity);
                    return databaseExecutor.executeUpdate(createBufferShardQuery);
                })
                .compose(v -> {
                    val createBufferQuery = adqmTablesSqlFactory.getCreateBufferSql(env, datamart, entity);
                    return databaseExecutor.executeUpdate(createBufferQuery);
                });
    }

    @Override
    public Future<Void> dropBufferTables(String env, String datamart, Entity entity) {
        val dropBufferQuery = adqmTablesSqlFactory.getDropBufferSql(env, datamart, entity);
        return databaseExecutor.executeUpdate(dropBufferQuery)
                .compose(v -> {
                    val dropBufferShardQuery = adqmTablesSqlFactory.getDropBufferShardSql(env, datamart, entity);
                    return databaseExecutor.executeUpdate(dropBufferShardQuery);
                });
    }

    @Override
    public Future<Void> closeVersionSqlByTableActual(String env, String datamart, Entity entity, long sysCn) {
        val query = adqmProcessingSqlFactory.getCloseVersionSqlByTableActual(env, datamart, entity, sysCn);
        return databaseExecutor.executeUpdate(query);
    }

    @Override
    public Future<Void> closeVersionSqlByTableBuffer(String env, String datamart, Entity entity, long sysCn) {
        val pkFields = String.join(",", EntityFieldUtils.getPkFieldNames(entity));
        val fields = String.join(",", EntityFieldUtils.getFieldNames(entity));
        val tableName = String.format("%s__%s.%s", env, datamart, entity.getName());
        val query = adqmProcessingSqlFactory.getCloseVersionSqlByTableBuffer(tableName, fields, pkFields, sysCn);
        return databaseExecutor.executeUpdate(query);
    }

    @Override
    public AdqmSharedProperties getSharedProperties() {
        return new AdqmSharedProperties(clickhouseProperties.getHosts(), clickhouseProperties.getUser(), clickhouseProperties.getPassword(),
                clickhouseProperties.getSocketTimeout(), clickhouseProperties.getDataTransferTimeout());
    }
}
