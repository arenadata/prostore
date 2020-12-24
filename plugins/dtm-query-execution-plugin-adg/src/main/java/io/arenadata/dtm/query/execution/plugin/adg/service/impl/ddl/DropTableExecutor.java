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

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.TtDeleteTablesQueueRequest;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.TtDeleteTablesRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtCartridgeProvider;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;
import static io.arenadata.dtm.query.execution.plugin.adg.constants.Procedures.DROP_SPACE;

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {

    private final QueryExecutorService executorService;
    private final TtCartridgeProvider cartridgeProvider;
    private final AdgCartridgeClient cartridgeClient;
    private final AdgHelperTableNamesFactory adgHelperTableNamesFactory;

    @Autowired
    public DropTableExecutor(QueryExecutorService executorService,
                             TtCartridgeProvider cartridgeProvider,
                             AdgCartridgeClient cartridgeClient,
                             AdgHelperTableNamesFactory adgHelperTableNamesFactory) {
        this.executorService = executorService;
        this.cartridgeProvider = cartridgeProvider;
        this.cartridgeClient = cartridgeClient;
        this.adgHelperTableNamesFactory = adgHelperTableNamesFactory;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        val tableNames = adgHelperTableNamesFactory.create(
                context.getRequest().getQueryRequest().getEnvName(),
                context.getRequest().getQueryRequest().getDatamartMnemonic(),
                context.getRequest().getEntity().getName());



        val request = new TtDeleteTablesRequest(Arrays.asList(
                tableNames.getStaging(),
                tableNames.getActual(),
                tableNames.getHistory()
        ));

        cartridgeClient.executeDeleteSpacesQueued(request, ar -> {
            if(ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Future<Object> dropSpacesFromDb(final Entity entity) {
        String actualTable = entity.getName() + ACTUAL_POSTFIX;
        String historyTable = entity.getName() + HISTORY_POSTFIX;
        String stagingTable = entity.getName() + STAGING_POSTFIX;
        // TODO It is better to drop all spaces at one, but currently it is not supported by cartridge
        return executorService.executeProcedure(DROP_SPACE, actualTable)
                .compose(f -> executorService.executeProcedure(DROP_SPACE, historyTable))
                .compose(f -> executorService.executeProcedure(DROP_SPACE, stagingTable));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adgDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
