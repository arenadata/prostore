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
package io.arenadata.dtm.query.execution.core.controller;

import io.arenadata.dtm.query.execution.core.service.metadata.DatamartMetaService;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

import java.util.List;

@Component
@Slf4j
public class DatamartMetaController {

    private final DatamartMetaService datamartMetaService;

    @Autowired
    public DatamartMetaController(DatamartMetaService datamartMetaService) {
        this.datamartMetaService = datamartMetaService;
    }

    public void getDatamartMeta(RoutingContext context) {
        datamartMetaService.getDatamartMeta(
                new ListToJsonHandler<>(context, "Reply sent with datamarts {}"));
    }

    public void getDatamartEntityMeta(RoutingContext context) {
        datamartMetaService.getEntitiesMeta(getDatamartMnemonic(context),
                new ListToJsonHandler<>(context, "Reply sent with entities {}"));
    }

    public void getEntityAttributesMeta(RoutingContext context) {
        datamartMetaService.getAttributesMeta(getDatamartMnemonic(context),
                getParam(context, RequestParam.ENTITY_MNEMONIC),
                new ListToJsonHandler<>(context, "Reply sent with attributes {}"));
    }

    private String getDatamartMnemonic(RoutingContext context) {
        return getParam(context, RequestParam.DATAMART_MNEMONIC);
    }

    private String getParam(RoutingContext context, String paramName) {
        return context.request().getParam(paramName);
    }

    private static class ListToJsonHandler<T> implements Handler<AsyncResult<List<T>>> {
        private final RoutingContext context;
        private String successLogMessage;

        ListToJsonHandler(RoutingContext context, String successLogMessage) {
            this.context = context;
            this.successLogMessage = successLogMessage;
        }

        @Override
        public void handle(AsyncResult<List<T>> asyncResult) {
            if (asyncResult.succeeded()) {
                String json = Json.encode(asyncResult.result());
                log.info(successLogMessage, json);
                context.response()
                        .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                        .setStatusCode(HttpResponseStatus.OK.code())
                        .end(json);
            } else {
                context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), asyncResult.cause());
            }
        }
    }
}
