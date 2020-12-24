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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_SHARD_POSTFIX;

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {
    private final static String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS %s__%s.%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;

    public DropTableExecutor(DatabaseExecutor databaseExecutor,
                             DdlProperties ddlProperties,
                             AppConfiguration appConfiguration) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
    }


    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        dropTable(context.getRequest().getEntity()).onComplete(handler);
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> dropTable(Entity entity) {
        String env = appConfiguration.getSystemName();

        String cluster = ddlProperties.getCluster();
        String schema = entity.getSchema();
        String table = entity.getName();

        String dropShard = String.format(DROP_TABLE_TEMPLATE, env, schema, table + ACTUAL_SHARD_POSTFIX, cluster);
        String dropDistributed = String.format(DROP_TABLE_TEMPLATE, env, schema, table + ACTUAL_POSTFIX, cluster);

        return databaseExecutor.executeUpdate(dropDistributed)
                .compose(v ->
                        databaseExecutor.executeUpdate(dropShard));
    }

}
