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
package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class RollbackRequest extends DatamartRequest {

    private String datamart;
    private String destinationTable;
    private long sysCn;
    private Entity entity;

    @Builder
    public RollbackRequest(QueryRequest queryRequest, String datamart, String destinationTable, long sysCn, Entity entity) {
        super(queryRequest);
        this.datamart = datamart;
        this.destinationTable = destinationTable;
        this.sysCn = sysCn;
        this.entity = entity;
    }
}
