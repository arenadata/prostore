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
package io.arenadata.dtm.query.execution.core.status.service;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.status.factory.StatusEventFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class StatusEventFactoryRegistryImpl implements StatusEventFactoryRegistry {
    private final Map<StatusEventCode, StatusEventFactory<?>> factoryMap;

    public StatusEventFactoryRegistryImpl() {
        factoryMap = new HashMap<>();
    }

    @Override
    public StatusEventFactory<?> get(StatusEventCode eventCode) {
        if (factoryMap.containsKey(eventCode)) {
            return factoryMap.get(eventCode);
        } else {
            throw new DtmException(String.format("StatusEventCode not supported: %s", eventCode));
        }
    }

    @Override
    public void registryFactory(StatusEventFactory<?> factory) {
        factoryMap.put(factory.getEventCode(), factory);
    }
}
