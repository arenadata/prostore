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
package io.arenadata.dtm.query.execution.core.config.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.execution.core.base.dto.request.CoreRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.ConfigRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.CONFIG;

@Data
@ToString
@EqualsAndHashCode(callSuper = true)
public class ConfigRequestContext extends CoreRequestContext<ConfigRequest, SqlConfigCall> {

    @Builder
    public ConfigRequestContext(RequestMetrics metrics,
                                ConfigRequest request,
                                SqlConfigCall sqlConfigCall,
                                String envName) {
        super(metrics, envName, request, sqlConfigCall);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return CONFIG;
    }

}
