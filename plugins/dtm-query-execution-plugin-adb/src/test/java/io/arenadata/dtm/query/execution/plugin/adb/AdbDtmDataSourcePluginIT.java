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
package io.arenadata.dtm.query.execution.plugin.adb;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = DtmTestConfiguration.class)
@ExtendWith(VertxExtension.class)
class AdbDtmDataSourcePluginIT {

    @Autowired
    private DdlService ddlService;

    private DtmDataSourcePlugin plugin = new DtmDataSourcePlugin() {

        @Override
        public boolean supports(SourceType sourceType) {
            return false;
        }

        @Override
        public SourceType getSourceType() {
            return SourceType.ADB;
        }

        @Override
        public void ddl(DdlRequestContext ddlRequest, Handler<AsyncResult<Void>> handler) {
            ddlService.execute(ddlRequest, handler);
        }

        @Override
        public void llr(LlrRequestContext llrRequest, Handler<AsyncResult<QueryResult>> handler) {

        }

        @Override
        public void mppr(MpprRequestContext mpprRequest, Handler<AsyncResult<QueryResult>> handler) {

        }

        @Override
        public void mppw(MppwRequestContext mppwRequest, Handler<AsyncResult<QueryResult>> handler) {

        }

        @Override
        public void calcQueryCost(QueryCostRequestContext queryCostRequest, Handler<AsyncResult<Integer>> handler) {

        }

        @Override
        public void status(StatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler) {

        }

        @Override
        public void rollback(RollbackRequestContext context, Handler<AsyncResult<Void>> asyncResultHandler) {

        }

        @Override
        public Set<String> getActiveCaches() {
            return Collections.singleton("adb_datamart");
        }

        @Override
        public Future<Void> checkTable(CheckContext context) {
            return null;
        }

        @Override
        public Future<Long> checkDataByCount(CheckDataByCountParams params) {
            return null;
        }

        @Override
        public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Params params) {
            return null;
        }

        @Override
        public Future<Void> truncateHistory(TruncateHistoryParams params) {
            return null;
        }
    };

    @Test
    void testDdl(VertxTestContext testContext) throws Throwable {
        Entity entity = new Entity("test.test_ts3222", Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "name", ColumnType.VARCHAR.name(), true, null, null, null),
                new EntityField(2, "dt", ColumnType.TIMESTAMP.name(), true, null, null, null)
        ));
        DdlRequest dto = new DdlRequest(null, entity);
        DdlRequestContext context = new DdlRequestContext(dto);
        context.setDdlType(DdlType.CREATE_TABLE);
        plugin.ddl(context, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }
}
