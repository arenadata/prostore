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
package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.query.execution.core.service.dml.InformationSchemaDefinitionService;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.stream.Collectors;

@Service
public class InformationSchemaDefinitionServiceImpl implements InformationSchemaDefinitionService {

    @Override
    public boolean isInformationSchemaRequest(QuerySourceRequest request) {
        Set<String> unicSchemes = request.getQueryRequest().getDeltaInformations().stream()
                .map(DeltaInformation::getSchemaName)
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        boolean informationSchemaExists = unicSchemes.contains(InformationSchemaView.SCHEMA_NAME);

        if (unicSchemes.size() > 1 && informationSchemaExists) {
            throw new IllegalArgumentException("Simultaneous query to the information schema and user schema isn't supported");
        } else {
            return informationSchemaExists;
        }
    }
}
