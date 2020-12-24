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
package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Request Mppw dto
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class MppwRequest extends DatamartRequest {

    /**
     * Sign of the start of mppw download
     */
    private Boolean isLoadStart;

    /**
     * Mppw params for loading to kafka
     */
    private MppwKafkaParameter kafkaParameter;

    @Builder
    public MppwRequest(QueryRequest queryRequest, Boolean isLoadStart, MppwKafkaParameter kafkaParameter) {
        super(queryRequest);
        this.isLoadStart = isLoadStart;
        this.kafkaParameter = kafkaParameter;
    }
}

