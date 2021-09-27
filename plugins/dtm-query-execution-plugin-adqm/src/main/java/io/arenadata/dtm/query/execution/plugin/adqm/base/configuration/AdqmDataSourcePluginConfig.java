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
package io.arenadata.dtm.query.execution.plugin.adqm.base.configuration;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.base.service.AdqmDtmDataSourcePlugin;
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
public class AdqmDataSourcePluginConfig {

    @Bean("adqmDtmDataSourcePlugin")
    public AdqmDtmDataSourcePlugin adqmDataSourcePlugin(
            @Qualifier("adqmDdlService") DdlService<Void> ddlService,
            @Qualifier("adqmLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adqmUpsertService") UpsertService upsertService,
            @Qualifier("adqmDeleteService") DeleteService deleteService,
            @Qualifier("adqmMpprService") MpprService mpprService,
            @Qualifier("adqmMppwService") MppwService mppwService,
            @Qualifier("adqmStatusService") StatusService statusService,
            @Qualifier("adqmRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adqmCheckTableService") CheckTableService checkTableService,
            @Qualifier("adqmCheckDataService") CheckDataService checkDataService,
            @Qualifier("adqmTruncateHistoryService") TruncateHistoryService truncateHistoryService,
            @Qualifier("adqmCheckVersionService") CheckVersionService checkVersionService,
            @Qualifier("adqmInitializationService") PluginInitializationService initializationService,
            @Qualifier("adqmSynchronizeService") SynchronizeService synchronizeService) {
        return new AdqmDtmDataSourcePlugin(
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
