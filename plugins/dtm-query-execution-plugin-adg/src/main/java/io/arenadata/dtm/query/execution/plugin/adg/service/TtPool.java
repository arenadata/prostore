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
package io.arenadata.dtm.query.execution.plugin.adg.service;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Реализация пула клиентов Tarantool
 */
public class TtPool extends GenericObjectPool<TtClient> {

  public TtPool(PooledObjectFactory<TtClient> factory) {
    super(factory);
  }

  public TtPool(PooledObjectFactory<TtClient> factory, GenericObjectPoolConfig<TtClient> config) {
    super(factory, config);
    setTestOnBorrow(true);
  }

  public TtPool(PooledObjectFactory<TtClient> factory, GenericObjectPoolConfig<TtClient> config, AbandonedConfig abandonedConfig) {
    super(factory, config, abandonedConfig);
  }
}
