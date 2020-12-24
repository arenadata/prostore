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
package io.arenadata.dtm.query.execution.core.dto.delta.query;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = false)
public class RollbackDeltaQuery extends DeltaQuery {

    private RequestMetrics requestMetrics;

    @Builder
    public RollbackDeltaQuery(QueryRequest request,
                              String datamart,
                              Long deltaNum,
                              LocalDateTime deltaDate) {
        super(request, datamart, deltaNum, deltaDate);
    }

    @Override
    public DeltaAction getDeltaAction() {
        return DeltaAction.ROLLBACK_DELTA;
    }
}
