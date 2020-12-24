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
package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgMppwKafkaContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AdgMppwKafkaContextFactoryImpl implements AdgMppwKafkaContextFactory {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    @Override
    public AdgMppwKafkaContext create(MppwRequest request) {
        val tableName = request.getKafkaParameter().getDestinationTableName();
        val datamart = request.getKafkaParameter().getDatamart();
        val envName = request.getQueryRequest().getEnvName();
        val helperTableNames = helperTableNamesFactory.create(envName, datamart, tableName);
        return new AdgMppwKafkaContext(
                request.getKafkaParameter().getTopic(),
                request.getKafkaParameter().getSysCn(),
                tableName,
                helperTableNames,
                new JsonObject(request.getKafkaParameter().getUploadMetadata().getExternalSchema())
        );
    }
}
