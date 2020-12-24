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
package io.arenadata.dtm.query.execution.plugin.adg.configuration;

import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtPool;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.TtClientFactory;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.TtResultTranslatorImpl;
import lombok.val;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(TarantoolDatabaseProperties.class)
public class TarantoolAutoConfiguration {

  @Bean("adgTtPool")
  public TtPool ttPool(TarantoolDatabaseProperties tarantoolProperties) {
    val resultTranslator = new TtResultTranslatorImpl();
    val factory = new TtClientFactory(tarantoolProperties, resultTranslator);
    val config = new GenericObjectPoolConfig<TtClient>();
    config.setJmxEnabled(false);
    return new TtPool(factory, config);
  }
}
