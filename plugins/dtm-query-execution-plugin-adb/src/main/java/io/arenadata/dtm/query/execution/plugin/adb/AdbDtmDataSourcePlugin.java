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

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;

import java.util.Collections;
import java.util.Set;

public class AdbDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public static final String ADB_DATAMART_CACHE = "adb_datamart";

    public AdbDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adbLlrService,
            MpprKafkaService<QueryResult> adbMpprKafkaService,
            MppwKafkaService<QueryResult> adbMppwKafkaService,
            QueryCostService<Integer> adbQueryCostService,
            StatusService<StatusQueryResult> statusService,
            RollbackService<Void> rollbackService,
            CheckTableService checkTableService,
            CheckDataService checkDataService,
            TruncateHistoryService truncateService) {
        super(ddlService,
                adbLlrService,
                adbMpprKafkaService,
                adbMppwKafkaService,
                adbQueryCostService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADB;
    }

    @Override
    public Set<String> getActiveCaches() {
        return Collections.singleton(ADB_DATAMART_CACHE);
    }
}
