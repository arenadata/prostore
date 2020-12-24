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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl.rollback;

import io.arenadata.dtm.common.exception.CrashException;
import io.arenadata.dtm.query.execution.plugin.adg.factory.ReverseHistoryTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service("adgRollbackService")
public class AdgRollbackService implements RollbackService<Void> {
    private static final String ERR_MSG = "Can't rollback delta";
    private final ReverseHistoryTransferRequestFactory requestFactory;
    private final AdgCartridgeClient cartridgeClient;

    @Override
    public void execute(RollbackRequestContext context, Handler<AsyncResult<Void>> handler) {
        try {
            cartridgeClient.reverseHistoryTransfer(requestFactory.create(context), ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture());
                } else {
                    log.error(ERR_MSG, ar.cause());
                    handler.handle(Future.failedFuture(new CrashException(ERR_MSG, ar.cause())));
                }
            });
        } catch (Exception ex) {
            log.error(ERR_MSG, ex);
            handler.handle(Future.failedFuture(new CrashException(ERR_MSG, ex)));
        }
    }
}
