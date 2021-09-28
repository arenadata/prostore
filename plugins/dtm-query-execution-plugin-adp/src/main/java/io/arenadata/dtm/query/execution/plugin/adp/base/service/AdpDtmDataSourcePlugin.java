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
package io.arenadata.dtm.query.execution.plugin.adp.base.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AdpDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public static final String ADP_QUERY_TEMPLATE_CACHE = "adpQueryTemplateCache";
    public static final String ADP_DATAMART_CACHE = "adp_datamart";

    public AdpDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adpLlrService,
            UpsertService upsertService,
            DeleteService deleteService,
            MpprService adpMpprService,
            MppwService adpMppwService,
            StatusService statusService,
            RollbackService<Void> rollbackService,
            CheckTableService checkTableService,
            CheckDataService checkDataService,
            TruncateHistoryService truncateService,
            CheckVersionService checkVersionService,
            PluginInitializationService initializationService,
            SynchronizeService synchronizeService) {
        super(ddlService,
                adpLlrService,
                upsertService,
                deleteService,
                adpMpprService,
                adpMppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                checkVersionService,
                truncateService,
                initializationService,
                synchronizeService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADP;
    }

    @Override
    public Set<String> getActiveCaches() {
        return new HashSet<>(Arrays.asList(ADP_DATAMART_CACHE, ADP_QUERY_TEMPLATE_CACHE));
    }
}
