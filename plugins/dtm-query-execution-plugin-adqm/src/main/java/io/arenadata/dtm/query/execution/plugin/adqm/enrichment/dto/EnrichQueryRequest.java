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
package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class EnrichQueryRequest {
    private List<DeltaInformation> deltaInformations;
    private List<Datamart> schema;
    private String envName;
    private SqlNode query;
    @Builder.Default
    private boolean isLocal;

    @Builder
    public EnrichQueryRequest(List<DeltaInformation> deltaInformations,
                              List<Datamart> schema,
                              boolean isLocal,
                              String envName,
                              SqlNode query) {
        this.deltaInformations = deltaInformations;
        this.schema = schema;
        this.isLocal = isLocal;
        this.envName = envName;
        this.query = query;
    }
}
