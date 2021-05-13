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

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Tarantool client pool
 */
public class AdgClientPool extends GenericObjectPool<AdgClient> {

    public AdgClientPool(PooledObjectFactory<AdgClient> factory) {
        super(factory);
    }

    public AdgClientPool(PooledObjectFactory<AdgClient> factory, GenericObjectPoolConfig<AdgClient> config) {
        super(factory, config);
        setTestOnBorrow(true);
    }

    public AdgClientPool(PooledObjectFactory<AdgClient> factory, GenericObjectPoolConfig<AdgClient> config, AbandonedConfig abandonedConfig) {
        super(factory, config, abandonedConfig);
    }
}
