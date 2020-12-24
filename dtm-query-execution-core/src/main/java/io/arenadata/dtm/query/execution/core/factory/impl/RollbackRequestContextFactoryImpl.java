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
package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.query.execution.core.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import org.springframework.stereotype.Component;

@Component
public class RollbackRequestContextFactoryImpl implements RollbackRequestContextFactory {

    @Override
    public RollbackRequestContext create(EdmlRequestContext context) {
        return new RollbackRequestContext(
                context.getMetrics(),
                RollbackRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .datamart(context.getSourceEntity().getSchema())
                .destinationTable(context.getDestinationEntity().getName())
                .sysCn(context.getSysCn())
                .entity(context.getDestinationEntity())
                .build());
    }
}
