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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
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

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory sqlFactory;

    @Autowired
    public DropTableExecutor(AdbQueryExecutor adbQueryExecutor, MetadataSqlFactory sqlFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlFactory = sqlFactory;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        try {
            String dropSql = sqlFactory.createDropTableScript(context.getRequest().getEntity());
            adbQueryExecutor.executeUpdate(dropSql, handler);
        } catch (Exception e) {
            log.error("Error executing drop table query!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adbDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
