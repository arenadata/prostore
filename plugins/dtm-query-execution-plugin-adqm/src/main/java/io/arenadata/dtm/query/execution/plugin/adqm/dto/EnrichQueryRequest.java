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
package io.arenadata.dtm.query.execution.plugin.adqm.dto;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrichQueryRequest {
    private QueryRequest queryRequest;
    private List<Datamart> schema;
    private boolean isLocal;

    public static EnrichQueryRequest generate(QueryRequest queryRequest, List<Datamart> schema, boolean isLocal) {
        return new EnrichQueryRequest(queryRequest, schema, isLocal);
    }

    public static EnrichQueryRequest generate(QueryRequest queryRequest, List<Datamart> schema) {
        return new EnrichQueryRequest(queryRequest, schema, false);
    }
}
