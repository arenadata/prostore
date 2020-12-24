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
package io.arenadata.dtm.query.execution.plugin.api.mppw;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.MPPW;

@ToString
public class MppwRequestContext extends RequestContext<MppwRequest> {

    public MppwRequestContext(RequestMetrics metrics, MppwRequest request) {
        super(metrics, request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return MPPW;
    }

    public MppwRequestContext copy() {
        return new MppwRequestContext(
                this.getMetrics(),
                new MppwRequest(
                        this.getRequest().getQueryRequest(),
                        this.getRequest().getIsLoadStart(),
                        this.getRequest().getKafkaParameter()));
    }
}
