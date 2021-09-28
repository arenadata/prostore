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
package io.arenadata.dtm.query.execution.core.status.factory.impl;

import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.StatusEventKey;
import io.arenadata.dtm.common.status.delta.CancelDeltaEvent;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaRecord;
import io.arenadata.dtm.query.execution.core.status.factory.AbstractStatusEventFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CancelDeltaStatusEventFactory extends AbstractStatusEventFactory<DeltaRecord, CancelDeltaEvent> {

    @Autowired
    protected CancelDeltaStatusEventFactory() {
        super(DeltaRecord.class);
    }

    @Override
    public StatusEventCode getEventCode() {
        return StatusEventCode.DELTA_CANCEL;
    }

    @Override
    protected CancelDeltaEvent createEventMessage(StatusEventKey eventKey, DeltaRecord eventData) {
        return new CancelDeltaEvent(eventData.getDeltaNum());
    }
}
