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
package io.arenadata.dtm.query.execution.plugin.adb.configuration;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.AdbDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdbDataSourcePluginConfig {

    @Bean("adbDtmDataSourcePlugin")
    public AdbDtmDataSourcePlugin adbDataSourcePlugin(
            @Qualifier("adbDdlService") DdlService<Void> ddlService,
            @Qualifier("adbLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adbMpprKafkaService") MpprKafkaService<QueryResult> mpprKafkaService,
            @Qualifier("adbMppwKafkaService") MppwKafkaService<QueryResult> mppwKafkaService,
            @Qualifier("adbQueryCostService") QueryCostService<Integer> queryCostService,
            @Qualifier("adbStatusService") StatusService<StatusQueryResult> statusService,
            @Qualifier("adbRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adbCheckTableService") CheckTableService checkTableService,
            @Qualifier("adbCheckDataService") CheckDataService checkDataService,
            @Qualifier("adbTruncateHistoryService") TruncateHistoryService truncateHistoryService) {
        return new AdbDtmDataSourcePlugin(
                ddlService,
                llrService,
                mpprKafkaService,
                mppwKafkaService,
                queryCostService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateHistoryService);
    }
}
