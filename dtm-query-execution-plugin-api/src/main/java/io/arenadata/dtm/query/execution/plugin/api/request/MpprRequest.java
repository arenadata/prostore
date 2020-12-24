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

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaParameter;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Request Mppr dto
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class MpprRequest extends DatamartRequest {

    /**
     * Mppr params for unloading from kafka
     */
    private MpprKafkaParameter kafkaParameter;
    /**
     * Logical schema
     */
    private List<Datamart> logicalSchema;
    /**
     * Column metadata list
     */
    private List<ColumnMetadata> metadata;
    /**
     * Destination entity
     */
    private Entity destinationEntity;

    @Builder
    public MpprRequest(QueryRequest queryRequest,
                       MpprKafkaParameter kafkaParameter,
                       List<Datamart> logicalSchema,
                       List<ColumnMetadata> metadata,
                       Entity destinationEntity) {
        super(queryRequest);
        this.kafkaParameter = kafkaParameter;
        this.logicalSchema = logicalSchema;
        this.metadata = metadata;
        this.destinationEntity = destinationEntity;
    }
}
