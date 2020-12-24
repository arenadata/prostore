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
package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Delta request executor
 */
public interface DeltaExecutor {

    /**
     * <p>Execute delta query</p>
     *
     * @param deltaQuery         delta query
     * @param asyncResultHandler asyncResultHandler
     */
    void execute(DeltaQuery deltaQuery, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * Get delta query action
     *
     * @return delta action type
     */
    DeltaAction getAction();
}
