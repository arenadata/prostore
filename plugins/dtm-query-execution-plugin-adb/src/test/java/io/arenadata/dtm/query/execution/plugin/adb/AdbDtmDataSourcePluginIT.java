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
package io.arenadata.dtm.query.execution.plugin.adb;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;
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
        public Future<Void> ddl(DdlRequest ddlRequest) {
            return ddlService.execute(ddlRequest);
        }

        @Override
        public Future<QueryResult> llr(LlrRequest llrRequest) {
            return null;
        }

        @Override
        public Future<QueryResult> llrEstimate(LlrRequest request) {
            return null;
        }

        @Override
        public Future<Void> prepareLlr(LlrRequest request) {
            return null;
        }

        @Override
        public Future<QueryResult> mppr(MpprRequest request) {
            return null;
        }

        @Override
        public Future<QueryResult> mppw(MppwRequest mppwRequest) {
            return null;
        }

        @Override
        public Future<StatusQueryResult> status(String topic) {
            return null;
        }

        @Override
        public Future<Void> rollback(RollbackRequest request) {
            return null;
        }

        @Override
        public Set<String> getActiveCaches() {
            return Collections.singleton("adb_datamart");
        }

        @Override
        public Future<Void> checkTable(CheckTableRequest request) {
            return null;
        }

        @Override
        public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
            return null;
        }

        @Override
        public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request params) {
            return null;
        }

        @Override
        public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
            return null;
        }

        @Override
        public Future<Void> truncateHistory(TruncateHistoryRequest params) {
            return null;
        }

        @Override
        public Future<Long> synchronize(SynchronizeRequest request) {
            return null;
        }

        @Override
        public Future<Void> initialize() {
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
        DdlRequest request = new DdlRequest(UUID.randomUUID(),
                "test",
                "test",
                entity,
                SqlKind.CREATE_TABLE);
        plugin.ddl(request)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }
}
