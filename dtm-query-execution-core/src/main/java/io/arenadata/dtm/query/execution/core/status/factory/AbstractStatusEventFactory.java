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
package io.arenadata.dtm.query.execution.core.status.factory;

import io.arenadata.dtm.common.configuration.core.CoreConstants;
import io.arenadata.dtm.common.status.PublishStatusEventRequest;
import io.arenadata.dtm.common.status.StatusEventKey;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.util.UUID;

public abstract class AbstractStatusEventFactory<IN, OUT> implements StatusEventFactory<OUT> {
    private final Class<IN> inClass;

    protected AbstractStatusEventFactory(Class<IN> inClass) {
        this.inClass = inClass;
    }

    protected abstract OUT createEventMessage(StatusEventKey eventKey, IN eventData);

    @Override
    @SneakyThrows
    public PublishStatusEventRequest<OUT> create(@NonNull String datamart, @NonNull String eventData) {
        val eventKey = getEventKey(datamart);
        IN readValue = DatabindCodec.mapper().readValue(eventData, inClass);
        return new PublishStatusEventRequest<>(eventKey, createEventMessage(eventKey, readValue));
    }

    @NotNull
    private StatusEventKey getEventKey(String datamart) {
        return new StatusEventKey(datamart,
                LocalDateTime.now(CoreConstants.CORE_ZONE_ID),
                getEventCode(),
                UUID.randomUUID());
    }
}
