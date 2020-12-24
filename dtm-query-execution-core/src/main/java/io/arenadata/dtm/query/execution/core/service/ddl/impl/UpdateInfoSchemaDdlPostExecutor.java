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
package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlPostExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UpdateInfoSchemaDdlPostExecutor implements DdlPostExecutor {

    private final InformationSchemaService informationSchemaService;

    @Autowired
    public UpdateInfoSchemaDdlPostExecutor(InformationSchemaService informationSchemaService) {
        this.informationSchemaService = informationSchemaService;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context) {
        return informationSchemaService.update(context.getQuery());
    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.UPDATE_INFORMATION_SCHEMA;
    }
}
