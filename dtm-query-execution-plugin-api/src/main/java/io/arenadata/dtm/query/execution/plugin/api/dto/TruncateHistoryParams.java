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
package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

import java.util.Optional;

@Getter
public class TruncateHistoryParams extends PluginParams {
    private final Optional<Long> sysCn;
    private final Entity entity;
    private final String env;
    private final Optional<SqlNode> conditions;

    public TruncateHistoryParams(SourceType type,
                                 RequestMetrics requestMetrics,
                                 Long sysCn,
                                 Entity entity,
                                 String env,
                                 SqlNode conditions) {
        super(type, requestMetrics);
        this.sysCn = Optional.ofNullable(sysCn);
        this.entity = entity;
        this.env = env;
        this.conditions = Optional.ofNullable(conditions);
    }
}
