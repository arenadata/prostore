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
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTables;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
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

@Component
@Slf4j
public class CreateTableExecutor implements DdlExecutor<Void> {
    private final DatabaseExecutor databaseExecutor;
    private final DropTableExecutor dropTableExecutor;
    private final CreateTableQueriesFactory<AdqmTables<String>> createTableQueriesFactory;

    public CreateTableExecutor(DatabaseExecutor databaseExecutor,
                               DropTableExecutor dropTableExecutor,
                               CreateTableQueriesFactory<AdqmTables<String>> createTableQueriesFactory) {
        this.databaseExecutor = databaseExecutor;
        this.dropTableExecutor = dropTableExecutor;
        this.createTableQueriesFactory = createTableQueriesFactory;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        Entity tbl = context.getRequest().getEntity();
        DdlRequestContext dropCtx = new DdlRequestContext(new DdlRequest(new QueryRequest(), tbl));

        dropTableExecutor.execute(dropCtx, SqlKind.DROP_TABLE.lowerName, ar -> createTable(context).onComplete(handler));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> createTable(DdlRequestContext context) {
        AdqmTables<String> createTableQueries = createTableQueriesFactory.create(context);
        return databaseExecutor.executeUpdate(createTableQueries.getShard())
                .compose(v -> databaseExecutor.executeUpdate(createTableQueries.getDistributed()));
    }
}
