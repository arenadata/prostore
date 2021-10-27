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
package io.arenadata.dtm.query.execution.plugin.adg.dml.service;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.adg.dml.service.upserts.DestinationUpsertSelectHandler;
import io.arenadata.dtm.query.execution.plugin.api.request.UpsertSelectRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.UpsertSelectService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("adgUpsertSelectService")
public class AdgUpsertSelectService implements UpsertSelectService {
    private final Map<SourceType, DestinationUpsertSelectHandler> handlers;

    public AdgUpsertSelectService(List<DestinationUpsertSelectHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(DestinationUpsertSelectHandler::getDestinations, handler -> handler));
    }

    @Override
    public Future<Void> execute(UpsertSelectRequest request) {
        List<Future> futures = new ArrayList<>();
        for (SourceType sourceType : request.getEntity().getDestination()) {
            val handler = handlers.get(sourceType);
            if (handler == null) {
                futures.add(Future.failedFuture(new DtmException(String.format("Upsert select from [ADG] to [%s] is not implemented", sourceType))));
                break;
            }

            futures.add(handler.handle(request));
        }

        return CompositeFuture.join(futures)
                .mapEmpty();
    }
}
