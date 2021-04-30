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
package io.arenadata.dtm.query.execution.plugin.api.mppr;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.UUID;


@Getter
@Setter
public class MpprRequest extends PluginRequest {
    private final SqlNode sqlNode;
    private final List<Datamart> logicalSchema;
    private final Entity destinationEntity;
    private final List<DeltaInformation> deltaInformations;
    private final ExternalTableLocationType externalTableLocationType;
    private List<ColumnMetadata> metadata;

    public MpprRequest(UUID requestId,
                       String envName,
                       String datamartMnemonic,
                       SqlNode sqlNode,
                       List<Datamart> logicalSchema,
                       List<ColumnMetadata> metadata,
                       Entity destinationEntity,
                       List<DeltaInformation> deltaInformations,
                       ExternalTableLocationType externalTableLocationType) {
        super(requestId, envName, datamartMnemonic);
        this.logicalSchema = logicalSchema;
        this.metadata = metadata;
        this.destinationEntity = destinationEntity;
        this.sqlNode = sqlNode;
        this.deltaInformations = deltaInformations;
        this.externalTableLocationType = externalTableLocationType;
    }
}
