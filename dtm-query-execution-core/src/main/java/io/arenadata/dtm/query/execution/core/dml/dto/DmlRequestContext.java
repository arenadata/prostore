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

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlUseSchema;
import io.arenadata.dtm.query.execution.core.base.dto.request.CoreRequestContext;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.DML;

@Getter
@Setter
@ToString
public class DmlRequestContext extends CoreRequestContext<DmlRequest, SqlNode> {

    private final SourceType sourceType;
    private final DmlType type;

    @Builder
    protected DmlRequestContext(RequestMetrics metrics,
                                String envName,
                                DmlRequest request,
                                SqlNode sqlNode,
                                SourceType sourceType) {
        super(metrics, envName, request, sqlNode);
        this.sourceType = sourceType;
        type = calculateType(sqlNode);
    }

    private DmlType calculateType(SqlNode sqlNode) {
        if (sqlNode instanceof SqlUseSchema) {
            return DmlType.USE;
        } else if (sqlNode instanceof SqlInsert) {
            return DmlType.UPSERT;
        } else if (sqlNode instanceof SqlDelete) {
            return DmlType.DELETE;
        } else  {
            return DmlType.LLR;
        }
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DML;
    }
}
