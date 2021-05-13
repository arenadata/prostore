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
package io.arenadata.dtm.query.execution.core.dml.dto;

import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class LlrRequestContext {
    private List<DeltaInformation> deltaInformations;
    private DmlRequestContext dmlRequestContext;
    private QuerySourceRequest sourceRequest;
    private SourceQueryTemplateValue queryTemplateValue;
    private RelRoot relNode;
    private SqlNode originalQuery;

    @Builder
    public LlrRequestContext(List<DeltaInformation> deltaInformations,
                             DmlRequestContext dmlRequestContext,
                             QuerySourceRequest sourceRequest,
                             SqlNode originalQuery) {
        this.deltaInformations = deltaInformations;
        this.dmlRequestContext = dmlRequestContext;
        this.sourceRequest = sourceRequest;
        this.originalQuery = originalQuery;
    }
}
