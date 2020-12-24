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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.TtDeleteTablesWithPrefixRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;


@Component
public class DropSchemaExecutor implements DdlExecutor<Void> {

    private final AdgCartridgeClient cartridgeClient;
    private final AdgHelperTableNamesFactory adgHelperTableNamesFactory;

    @Autowired
    public DropSchemaExecutor(AdgCartridgeClient cartridgeClient,
                              AdgHelperTableNamesFactory adgHelperTableNamesFactory) {
        this.cartridgeClient = cartridgeClient;
        this.adgHelperTableNamesFactory = adgHelperTableNamesFactory;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {

        val tableNames = adgHelperTableNamesFactory.create(
                context.getRequest().getQueryRequest().getEnvName(),
                context.getRequest().getQueryRequest().getDatamartMnemonic(),
                "table");

        val request = new TtDeleteTablesWithPrefixRequest(tableNames.getPrefix());

        cartridgeClient.executeDeleteSpacesWithPrefixQueued(request, ar -> {
            if(ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adgDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
