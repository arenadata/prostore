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
package io.arenadata.dtm.query.execution.plugin.api.check;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckCall;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;

public class CheckContext extends RequestContext<DatamartRequest> {
    private Entity entity;
    private CheckType checkType;
    private SqlCheckCall sqlCheckCall;

    public CheckContext(RequestMetrics metrics,
                        DatamartRequest request,
                        CheckType checkType,
                        SqlCheckCall sqlCheckCall) {
        super(metrics, request);
        this.checkType = checkType;
        this.sqlCheckCall = sqlCheckCall;
    }

    public CheckContext(RequestMetrics metrics, DatamartRequest request, Entity entity) {
        super(metrics, request);
        this.entity = entity;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.CHECK;
    }

    public Entity getEntity() {
        return entity;
    }

    public CheckType getCheckType() {
        return checkType;
    }

    public SqlCheckCall getSqlCheckCall() {
        return sqlCheckCall;
    }
}
