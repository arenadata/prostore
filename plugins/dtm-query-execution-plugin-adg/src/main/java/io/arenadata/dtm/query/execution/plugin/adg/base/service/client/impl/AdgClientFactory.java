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
package io.arenadata.dtm.query.execution.plugin.adg.base.service.client.impl;

import io.arenadata.dtm.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgClient;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgResultTranslator;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class AdgClientFactory extends BasePooledObjectFactory<AdgClient> {

    private final TarantoolDatabaseProperties tarantoolProperties;
    private final AdgResultTranslator resultTranslator;

    public AdgClientFactory(TarantoolDatabaseProperties tarantoolProperties, AdgResultTranslator resultTranslator) {
        this.tarantoolProperties = tarantoolProperties;
        this.resultTranslator = resultTranslator;
    }

    @Override
    public AdgClient create() {
        AdgClient client;
        try {
            client = new AdgClientImpl(tarantoolProperties, resultTranslator);
        } catch (Exception e) {
            throw new DataSourceException(String.format("Error connecting to Tarantool: %s",
                    tarantoolProperties), e);
        }
        return client;
    }

    @Override
    public PooledObject<AdgClient> wrap(AdgClient adgClient) {
        return new DefaultPooledObject<>(adgClient);
    }

    @Override
    public void destroyObject(PooledObject<AdgClient> p) {
        p.getObject().close();
    }

    @Override
    public boolean validateObject(PooledObject<AdgClient> p) {
        return p.getObject().isAlive();
    }
}
