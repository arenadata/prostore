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
package io.arenadata.dtm.query.execution.core.configuration;

import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.configuration.properties.ServiceDbZookeeperProperties;
import io.arenadata.dtm.query.execution.core.AbstractCoreDtmIT;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.Objects;

@TestConfiguration
@Slf4j
public class IntegrationTestConfiguration {

    @Bean(destroyMethod = "close", name = "itTestVertx")
    public Vertx vertx() {
        System.setProperty("org.vertx.logger-delegate-factory-class-name",
                "org.vertx.java.core.logging.impl.SLF4JLogDelegateFactory");
        return Vertx.vertx();
    }

    @Bean("itTestZkProvider")
    public ZookeeperConnectionProvider zkConnectionProvider() {
        final ServiceDbZookeeperProperties zookeeperProperties = new ServiceDbZookeeperProperties();
        zookeeperProperties.setConnectionString(AbstractCoreDtmIT.getZkDsConnectionStringAsExternal());
        zookeeperProperties.setChroot(
                Objects.requireNonNull(AbstractCoreDtmIT.dtmProperties
                        .getProperty("core.datasource.zookeeper.chroot")).toString());
        return new ZookeeperConnectionProviderImpl(zookeeperProperties,
                Objects.requireNonNull(AbstractCoreDtmIT.dtmProperties
                        .getProperty("core.env.name")).toString());
    }

    @Bean("itTestZkKafkaProvider")
    public KafkaZookeeperConnectionProvider zkKafkaConnectionProvider() {
        final KafkaZookeeperProperties zookeeperProperties = new KafkaZookeeperProperties();
        zookeeperProperties.setConnectionString(AbstractCoreDtmIT.getZkKafkaConnectionString());
        return new KafkaZookeeperConnectionProviderImpl(zookeeperProperties);
    }


    @Bean("itTestZkExecutor")
    public ZookeeperExecutor zkExecutor(@Qualifier("itTestZkProvider") ZookeeperConnectionProvider zookeeperConnectionProvider) {
        return new ZookeeperExecutorImpl(zookeeperConnectionProvider, vertx());
    }

    @Bean("itTestWebClient")
    public WebClient webClient() {
        final WebClientOptions options = new WebClientOptions();
        options.setMaxPoolSize(10);
        return WebClient.create(vertx(), options);
    }
}
