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
package io.arenadata.dtm.query.execution.core.verticle;

import com.google.common.net.HttpHeaders;
import io.arenadata.dtm.query.execution.core.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.controller.DatamartMetaController;
import io.arenadata.dtm.query.execution.core.controller.MetricsController;
import io.arenadata.dtm.query.execution.core.controller.QueryController;
import io.arenadata.dtm.query.execution.core.controller.RequestParam;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@Component
@Slf4j
public class QueryVerticle extends AbstractVerticle {

    private final AppConfiguration configuration;
    private final DatamartMetaController datamartMetaController;
    private final QueryController queryController;
    private final MetricsController metricsController;

    @Autowired
    public QueryVerticle(AppConfiguration configuration,
                         DatamartMetaController datamartMetaController,
                         QueryController queryController,
                         MetricsController metricsController) {
        this.configuration = configuration;
        this.datamartMetaController = datamartMetaController;
        this.queryController = queryController;
        this.metricsController = metricsController;
    }

    @Override
    public void start() {
        Router router = Router.router(vertx);
        router.mountSubRouter("/", apiRouter());
        HttpServer httpServer = vertx.createHttpServer().requestHandler(router)
                .listen(configuration.httpPort());
        log.info("The server is running on the port: {}", httpServer.actualPort());
    }

    private Router apiRouter() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().consumes(APPLICATION_JSON_VALUE);
        router.route().produces(APPLICATION_JSON_VALUE);
        router.route().failureHandler(ctx -> {
            final JsonObject error = new JsonObject()
                    .put("exceptionMessage", ctx.failure().getMessage());
            ctx.response().setStatusCode(ctx.statusCode());
            ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE);
            ctx.response().end(error.encode());
        });
        router.get("/meta").handler(datamartMetaController::getDatamartMeta);
        router.get(String.format("/meta/:%s/entities", RequestParam.DATAMART_MNEMONIC))
                .handler(datamartMetaController::getDatamartEntityMeta);
        router.get(String.format("/meta/:%s/entity/:%s/attributes",
                RequestParam.DATAMART_MNEMONIC, RequestParam.ENTITY_MNEMONIC))
                .handler(datamartMetaController::getEntityAttributesMeta);
        router.post("/query/execute").handler(queryController::executeQueryWithoutParams);
        router.put("/metrics/turn/on").handler(metricsController::turnOn);
        router.put("/metrics/turn/off").handler(metricsController::turnOff);
        return router;
    }
}
