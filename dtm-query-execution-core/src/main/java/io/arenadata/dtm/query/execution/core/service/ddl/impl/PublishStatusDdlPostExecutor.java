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
package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.eventbus.DataHeader;
import io.arenadata.dtm.common.eventbus.DataTopic;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.ddl.DatamartSchemaChangedEvent;
import io.arenadata.dtm.query.execution.core.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlPostExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class PublishStatusDdlPostExecutor implements DdlPostExecutor {
    private final Vertx vertx;
    private final DtmConfig dtmConfig;

    @Autowired
    public PublishStatusDdlPostExecutor(@Qualifier("coreVertx") Vertx vertx,
                                        AppConfiguration appConfiguration) {
        this.vertx = vertx;
        this.dtmConfig = appConfiguration.dtmSettings();
    }

    @Override
    public Future<Void> execute(DdlRequestContext context) {
        try {
            DatamartSchemaChangedEvent eventData = DatamartSchemaChangedEvent.builder()
                    .datamart(context.getDatamartName())
                    .changeDateTime(LocalDateTime.now(dtmConfig.getTimeZone()))
                    .build();
            val message = DatabindCodec.mapper().writeValueAsString(eventData);
            val options = new DeliveryOptions();
            options.addHeader(DataHeader.DATAMART.getValue(), context.getDatamartName());
            options.addHeader(DataHeader.STATUS_EVENT_CODE.getValue(), StatusEventCode.DATAMART_SCHEMA_CHANGED.name());
            vertx.eventBus().send(DataTopic.STATUS_EVENT_PUBLISH.getValue(), message, options);
            return Future.succeededFuture();
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.PUBLISH_STATUS;
    }
}
