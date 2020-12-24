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

import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtResultTranslator;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class TtClientFactory extends BasePooledObjectFactory<TtClient> {

  private TarantoolDatabaseProperties tarantoolProperties;
  private TtResultTranslator resultTranslator;

  public TtClientFactory(TarantoolDatabaseProperties tarantoolProperties, TtResultTranslator resultTranslator) {
    this.tarantoolProperties = tarantoolProperties;
    this.resultTranslator = resultTranslator;
  }

  @Override
  public TtClient create() {
    TtClient client;
    try {
      client = new TtClientImpl(tarantoolProperties, resultTranslator);
    } catch (Exception e) {
      throw new RuntimeException("Error connecting to Tarantool: " + tarantoolProperties, e);
    }
    return client;
  }

  @Override
  public PooledObject<TtClient> wrap(TtClient ttClient) {
    return new DefaultPooledObject<>(ttClient);
  }

  @Override
  public void destroyObject(PooledObject<TtClient> p) {
    p.getObject().close();
  }

  @Override
  public boolean validateObject(PooledObject<TtClient> p) {
    return p.getObject().isAlive();
  }
}
