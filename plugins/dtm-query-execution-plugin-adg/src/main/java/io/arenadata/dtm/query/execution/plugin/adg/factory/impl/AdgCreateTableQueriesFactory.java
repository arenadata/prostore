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

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.dto.AdgTables;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.*;
import io.arenadata.dtm.query.execution.plugin.adg.utils.AdgUtils;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.function.Function;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Service("adgCreateTableQueriesFactory")
public class AdgCreateTableQueriesFactory implements CreateTableQueriesFactory<AdgTables<AdgSpace>> {
    private final TableEntitiesFactory<AdgTables<Space>> tableEntitiesFactory;

    @Autowired
    public AdgCreateTableQueriesFactory(TableEntitiesFactory<AdgTables<Space>> tableEntitiesFactory) {
        this.tableEntitiesFactory = tableEntitiesFactory;
    }

    @Override
    public AdgTables<AdgSpace> create(DdlRequestContext context) {
        QueryRequest queryRequest = context.getRequest().getQueryRequest();
        Entity entity = context.getRequest().getEntity();
        AdgTables<Space> tableEntities = tableEntitiesFactory.create(entity, queryRequest.getEnvName());
        Function<String, String> getName = postfix ->
                AdgUtils.getSpaceName(queryRequest.getEnvName(), queryRequest.getDatamartMnemonic(), entity.getName(),
                        postfix);
        return new AdgTables<>(
                new AdgSpace(getName.apply(ACTUAL_POSTFIX), tableEntities.getActual()),
                new AdgSpace(getName.apply(HISTORY_POSTFIX), tableEntities.getHistory()),
                new AdgSpace(getName.apply(STAGING_POSTFIX), tableEntities.getStaging())
        );
    }
}
