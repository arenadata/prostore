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
package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmHelperTableNames;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmHelperTableNamesFactory;
import org.springframework.stereotype.Component;

@Component
public class AdqmHelperTableNamesFactoryImpl implements AdqmHelperTableNamesFactory {

    @Override
    public AdqmHelperTableNames create(String envName, String datamartMnemonic, String tableName) {
        String schema = envName + "__" + datamartMnemonic;
        return new AdqmHelperTableNames(
            schema,
            tableName + "_actual",
            tableName + "_actual_shard"
        );
    }
}
