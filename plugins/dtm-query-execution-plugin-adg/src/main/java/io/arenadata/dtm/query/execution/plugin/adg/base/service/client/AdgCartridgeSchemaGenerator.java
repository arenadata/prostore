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
package io.arenadata.dtm.query.execution.plugin.adg.base.service.client;

import io.arenadata.dtm.query.execution.plugin.adg.base.dto.AdgTables;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema.AdgSpace;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.stream.Stream;

@Service
public class AdgCartridgeSchemaGenerator {
    private final CreateTableQueriesFactory<AdgTables<AdgSpace>> createTableQueriesFactory;

    @Autowired
    public AdgCartridgeSchemaGenerator(CreateTableQueriesFactory<AdgTables<AdgSpace>> createTableQueriesFactory) {
        this.createTableQueriesFactory = createTableQueriesFactory;
    }

    public Future<OperationYaml> generate(DdlRequest request, OperationYaml yaml) {
        return Future.future(promise -> {
            if (yaml.getSpaces() == null) {
                yaml.setSpaces(new LinkedHashMap<>());
            }
            val spaces = yaml.getSpaces();
            AdgTables<AdgSpace> adgCreateTableQueries = createTableQueriesFactory.create(request.getEntity(), request.getEnvName());
            Stream.of(adgCreateTableQueries.getActual(), adgCreateTableQueries.getHistory(),
                    adgCreateTableQueries.getStaging())
                    .forEach(space -> spaces.put(space.getName(), space.getSpace()));
            promise.complete(yaml);
        });
    }

}
