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
package io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.dto.AdgMppwKafkaContext;
import io.arenadata.dtm.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.factory.AdgMppwKafkaContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AdgMppwKafkaContextFactoryImpl implements AdgMppwKafkaContextFactory {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    @Override
    public AdgMppwKafkaContext create(MppwKafkaRequest request) {
        val tableName = request.getDestinationTableName();
        val helperTableNames = helperTableNamesFactory.create(
                request.getEnvName(),
                request.getDatamartMnemonic(),
                tableName);
        return new AdgMppwKafkaContext(
                request.getTopic(),
                request.getSysCn(),
                tableName,
                helperTableNames,
                new JsonObject(request.getUploadMetadata().getExternalSchema())
        );
    }
}
