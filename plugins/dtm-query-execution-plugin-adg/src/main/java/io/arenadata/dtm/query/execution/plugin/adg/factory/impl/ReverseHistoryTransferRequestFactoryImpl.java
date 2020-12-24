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
package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.AdgRollbackProperties;
import io.arenadata.dtm.query.execution.plugin.adg.dto.rollback.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.factory.ReverseHistoryTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ReverseHistoryTransferRequestFactoryImpl implements ReverseHistoryTransferRequestFactory {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;
    private final AdgRollbackProperties rollbackProperties;

    @Override
    public ReverseHistoryTransferRequest create(RollbackRequestContext context) {
        val envName = context.getRequest().getQueryRequest().getEnvName();
        val tableName = context.getRequest().getDestinationTable();
        val datamart = context.getRequest().getDatamart();
        val helperTableNames = helperTableNamesFactory.create(envName, datamart, tableName);
        return ReverseHistoryTransferRequest.builder()
            .eraseOperationBatchSize(rollbackProperties.getEraseOperationBatchSize())
            .stagingTableName(helperTableNames.getStaging())
            .historyTableName(helperTableNames.getHistory())
            .actualTableName(helperTableNames.getActual())
            .sysCn(context.getRequest().getSysCn())
            .build();
    }
}
