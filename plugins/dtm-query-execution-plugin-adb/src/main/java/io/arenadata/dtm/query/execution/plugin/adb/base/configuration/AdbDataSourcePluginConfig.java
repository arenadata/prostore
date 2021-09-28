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
package io.arenadata.dtm.query.execution.plugin.adb.base.configuration;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.AdbDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdbDataSourcePluginConfig {

    @Bean("adbDtmDataSourcePlugin")
    public AdbDtmDataSourcePlugin adbDataSourcePlugin(
            @Qualifier("adbDdlService") DdlService<Void> ddlService,
            @Qualifier("adbLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adbUpsertService") UpsertService upsertService,
            @Qualifier("adbDeleteService") DeleteService deleteService,
            @Qualifier("adbMpprService") MpprService mpprService,
            @Qualifier("adbMppwService") MppwService mppwService,
            @Qualifier("adbStatusService") StatusService statusService,
            @Qualifier("adbRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adbCheckTableService") CheckTableService checkTableService,
            @Qualifier("adbCheckDataService") CheckDataService checkDataService,
            @Qualifier("adbTruncateHistoryService") TruncateHistoryService truncateHistoryService,
            @Qualifier("adbCheckVersionService") CheckVersionService checkVersionService,
            @Qualifier("adbInitializationService") PluginInitializationService initializationService,
            @Qualifier("adbSynchronizeService") SynchronizeService synchronizeService) {
        return new AdbDtmDataSourcePlugin(
                ddlService,
                llrService,
                upsertService,
                deleteService,
                mpprService,
                mppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateHistoryService,
                checkVersionService,
                initializationService,
                synchronizeService);
    }
}
