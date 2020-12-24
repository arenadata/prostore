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
package io.arenadata.dtm.query.execution.core.configuration.vertx;

import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.RestoreStateService;
import io.arenadata.dtm.query.execution.core.verticle.QueryVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class VertxConfiguration implements ApplicationListener<ApplicationReadyEvent> {

    @Bean("coreVertx")
    @ConditionalOnMissingBean(Vertx.class)
    public Vertx vertx() {
        return Vertx.vertx();
    }

    /**
     * Centrally sets all verticals strictly after up all configurations. In contrast to the
     * initMethod call, it guarantees the order of the vertical deployment, since the @PostConstruct phase is executed only within the
     * configuration.
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        val informationSchemaService = event.getApplicationContext().getBean(InformationSchemaService.class);
        val restoreStateService = event.getApplicationContext().getBean(RestoreStateService.class);
        val vertx = event.getApplicationContext().getBean("coreVertx", Vertx.class);
        val verticles = event.getApplicationContext().getBeansOfType(Verticle.class);
        val queryVerticle = verticles.remove(QueryVerticle.class.getName());

        informationSchemaService.createInformationSchemaViews()
                .compose(v -> deployVerticle(vertx, verticles.values()))
                .compose(v -> {
                    restoreStateService.restoreState()
                            .onFailure(fail -> {
                                log.error("Error in restoring state", fail);
                            });
                    return deployVerticle(vertx, Collections.singletonList(queryVerticle));
                })
                .onSuccess(success -> {
                    log.debug("Dtm started successfully");
                })
                .onFailure(err -> {
                    val exitCode = SpringApplication.exit(event.getApplicationContext(), () -> 1);
                    System.exit(exitCode);
                });
    }

    private Future<Object> deployVerticle(Vertx vertx, Collection<Verticle> verticles) {
        log.info("Verticals found: {}", verticles.size());
        return CompositeFuture.join(verticles.stream()
                .map(verticle -> Future.future(p -> {
                    vertx.deployVerticle(verticle, ar -> {
                        if (ar.succeeded()) {
                            log.debug("Vertical '{}' deployed successfully", verticle.getClass().getName());
                            p.complete();
                        } else {
                            log.error("Vertical deploy error", ar.cause());
                            p.fail(ar.cause());
                        }
                    });
                }))
                .collect(Collectors.toList()))
                .mapEmpty();
    }
}
