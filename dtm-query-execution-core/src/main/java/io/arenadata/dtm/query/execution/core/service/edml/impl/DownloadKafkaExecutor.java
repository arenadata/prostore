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
package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.configuration.properties.EdmlProperties;
import io.arenadata.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.service.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.core.service.impl.CheckColumnTypesServiceImpl;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DownloadKafkaExecutor implements EdmlDownloadExecutor {

    private final MpprKafkaRequestFactory mpprKafkaRequestFactory;
    private final ColumnMetadataService columnMetadataService;
    private final DataSourcePluginService pluginService;
    private final EdmlProperties edmlProperties;
    private final CheckColumnTypesService checkColumnTypesService;

    @Autowired
    public DownloadKafkaExecutor(DataSourcePluginService pluginService,
                                 MpprKafkaRequestFactory mpprKafkaRequestFactory,
                                 EdmlProperties edmlProperties,
                                 CheckColumnTypesService checkColumnTypesService,
                                 ColumnMetadataService columnMetadataService) {
        this.pluginService = pluginService;
        this.mpprKafkaRequestFactory = mpprKafkaRequestFactory;
        this.columnMetadataService = columnMetadataService;
        this.edmlProperties = edmlProperties;
        this.checkColumnTypesService = checkColumnTypesService;
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        execute(context).onComplete(resultHandler);
    }

    private Future<QueryResult> execute(EdmlRequestContext context) {
        if (checkDestinationType(context)) {
            QueryParserRequest queryParserRequest = new QueryParserRequest(context.getRequest().getQueryRequest(),
                    context.getLogicalSchema());
            return checkColumnTypesService.check(context.getDestinationEntity().getFields(), queryParserRequest)
                    .compose(areEqual -> areEqual ? mpprKafkaRequestFactory.create(context)
                            : Future.failedFuture(String.format(CheckColumnTypesServiceImpl.FAIL_CHECK_COLUMNS_PATTERN,
                            context.getDestinationEntity().getName())))
                    .compose(mpprRequestContext -> initColumnMetadata(context, mpprRequestContext))
                    .compose(this::executeMppr);
        } else {
            return Future.failedFuture(new IllegalStateException(
                    String.format("Source not exist in [%s]", edmlProperties.getSourceType())));
        }
    }

    private boolean checkDestinationType(EdmlRequestContext context) {
        return context.getLogicalSchema().stream()
            .flatMap(datamart -> datamart.getEntities().stream())
            .allMatch(entity -> entity.getDestination().contains(edmlProperties.getSourceType()));
    }

    private Future<MpprRequestContext> initColumnMetadata(EdmlRequestContext context,
                                                          MpprRequestContext mpprRequestContext) {
        return Future.future((Promise<MpprRequestContext> promise) -> {
            val parserRequest = new QueryParserRequest(context.getRequest().getQueryRequest(), context.getLogicalSchema());
            columnMetadataService.getColumnMetadata(parserRequest, ar -> {
                if (ar.succeeded()) {
                    mpprRequestContext.getRequest().setMetadata(ar.result());
                    promise.complete(mpprRequestContext);
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private Future<QueryResult> executeMppr(MpprRequestContext mpprRequestContext) {
        return Future.future(promise -> pluginService.mppr(edmlProperties.getSourceType(),
                mpprRequestContext, promise));
    }

    @Override
    public ExternalTableLocationType getDownloadType() {
        return ExternalTableLocationType.KAFKA;
    }
}
