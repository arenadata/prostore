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
package io.arenadata.dtm.query.execution.plugin.api.mppw;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.*;

import java.util.UUID;

/**
 * Request Mppw dto
 */
@Data
@ToString
@EqualsAndHashCode(callSuper = true)
public class MppwRequest extends PluginRequest {

    protected final Entity sourceEntity;
    protected final Long sysCn;
    protected final String destinationTableName;
    protected final BaseExternalEntityMetadata uploadMetadata;
    protected final ExternalTableLocationType externalTableLocationType;
    /**
     * Sign of the start of mppw download
     */
    protected Boolean isLoadStart;

    public MppwRequest(UUID requestId,
                       String envName,
                       String datamartMnemonic,
                       Boolean isLoadStart,
                       Entity sourceEntity,
                       Long sysCn,
                       String destinationTableName,
                       BaseExternalEntityMetadata uploadMetadata,
                       ExternalTableLocationType externalTableLocationType) {
        super(requestId, envName, datamartMnemonic);
        this.isLoadStart = isLoadStart;
        this.sourceEntity = sourceEntity;
        this.sysCn = sysCn;
        this.destinationTableName = destinationTableName;
        this.uploadMetadata = uploadMetadata;
        this.externalTableLocationType = externalTableLocationType;
    }
}

