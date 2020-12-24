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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.query.execution.plugin.adg.dto.AdgTables;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.AdgSpace;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.stream.Stream;

@Service
public class TtCartridgeSchemaGeneratorImpl implements TtCartridgeSchemaGenerator {
    private final CreateTableQueriesFactory<AdgTables<AdgSpace>> createTableQueriesFactory;

    @Autowired
    public TtCartridgeSchemaGeneratorImpl(CreateTableQueriesFactory<AdgTables<AdgSpace>> createTableQueriesFactory) {
        this.createTableQueriesFactory = createTableQueriesFactory;
    }

    @Override
    public void generate(DdlRequestContext context, OperationYaml yaml, Handler<AsyncResult<OperationYaml>> handler) {
        if (yaml.getSpaces() == null) {
            yaml.setSpaces(new LinkedHashMap<>());
        }
        val spaces = yaml.getSpaces();
        AdgTables<AdgSpace> adgCreateTableQueries = createTableQueriesFactory.create(context);
        Stream.of(adgCreateTableQueries.getActual(), adgCreateTableQueries.getHistory(),
                adgCreateTableQueries.getStaging())
                .forEach(space -> spaces.put(space.getName(), space.getSpace()));
        handler.handle(Future.succeededFuture(yaml));
    }

}
