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
package io.arenadata.dtm.query.execution.core.edml.mppw.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlAction;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.factory.RollbackWriteOpsQueryResultFactory;
import io.arenadata.dtm.query.execution.core.edml.service.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RollbackCrashedWriteOpExecutor implements EdmlExecutor {

    private final RestoreStateService restoreStateService;
    private final RollbackWriteOpsQueryResultFactory writeOpsQueryResultFactory;

    @Autowired
    public RollbackCrashedWriteOpExecutor(RestoreStateService restoreStateService,
                                          RollbackWriteOpsQueryResultFactory writeOpsQueryResultFactory) {
        this.restoreStateService = restoreStateService;
        this.writeOpsQueryResultFactory = writeOpsQueryResultFactory;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        if (StringUtils.isEmpty(context.getRequest().getQueryRequest().getDatamartMnemonic())) {
            String errMsg = "Datamart must not be empty!";
            return Future.failedFuture(new DtmException(errMsg));
        } else {
            return restoreStateService.restoreErase(context.getRequest().getQueryRequest().getDatamartMnemonic())
                    .map(writeOpsQueryResultFactory::create);
        }
    }

    @Override
    public EdmlAction getAction() {
        return EdmlAction.ROLLBACK;
    }
}
