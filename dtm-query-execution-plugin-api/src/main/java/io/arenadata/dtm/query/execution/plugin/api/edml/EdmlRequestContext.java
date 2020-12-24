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
package io.arenadata.dtm.query.execution.plugin.api.edml;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.sql.SqlInsert;

import java.util.List;

import static io.arenadata.dtm.common.model.SqlProcessingType.EDML;

@Getter
@Setter
@ToString
public class EdmlRequestContext extends RequestContext<DatamartRequest> {
    private Entity sourceEntity;
    private Entity destinationEntity;
    private Long sysCn;
    private final SqlInsert sqlNode;
    private String dmlSubquery;
    private List<Datamart> logicalSchema;

    public EdmlRequestContext(RequestMetrics metrics, DatamartRequest request, SqlInsert sqlNode) {
        super(metrics, request);
        this.sqlNode = sqlNode;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return EDML;
    }

}
