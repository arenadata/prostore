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
package io.arenadata.dtm.query.execution.core.edml.mppr.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlSelectExt;
import io.arenadata.dtm.query.execution.core.edml.configuration.EdmlProperties;
import io.arenadata.dtm.query.execution.core.edml.mppr.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.edml.mppr.service.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesServiceImpl;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
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
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return executeInternal(context);
    }

    private Future<QueryResult> executeInternal(EdmlRequestContext context) {
        val actualDatasourceType = getActualDatasourceType(context.getSqlNode());
        if (checkDestinationType(context, actualDatasourceType)) {
            val queryParserRequest = new QueryParserRequest(context.getDmlSubQuery(), context.getLogicalSchema());
            //TODO add checking for column names, and throw new ColumnNotExistsException if will be error
            return checkColumnTypesService.check(context.getDestinationEntity().getFields(), queryParserRequest)
                    .compose(areEqual -> areEqual ? mpprKafkaRequestFactory.create(context)
                            : Future.failedFuture(getFailCheckColumnsException(context)))
                    .compose(mpprKafkaRequest -> initColumnMetadata(context, mpprKafkaRequest))
                    .compose(mpprKafkaRequest ->
                            pluginService.mppr(actualDatasourceType, context.getMetrics(), mpprKafkaRequest));
        } else {
            return Future.failedFuture(new DtmException(
                    String.format("Queried entity is missing for the specified DATASOURCE_TYPE %s", actualDatasourceType)));
        }
    }

    private SourceType getActualDatasourceType(SqlNode sqlInsert) {
        val queryDatasourceType = ((SqlSelectExt) ((SqlInsert) sqlInsert).getSource()).getDatasourceType();
        return queryDatasourceType == null ? edmlProperties.getSourceType() : SourceType.valueOfAvailable(queryDatasourceType.getNlsString().getValue());
    }

    private DtmException getFailCheckColumnsException(EdmlRequestContext context) {
        return new DtmException(String.format(CheckColumnTypesServiceImpl.FAIL_CHECK_COLUMNS_PATTERN,
                context.getDestinationEntity().getName()));
    }

    private boolean checkDestinationType(EdmlRequestContext context, SourceType actualSourceType) {
        return context.getLogicalSchema().stream()
                .flatMap(datamart -> datamart.getEntities().stream())
                .allMatch(entity -> entity.getDestination().contains(actualSourceType));
    }

    private Future<MpprRequest> initColumnMetadata(EdmlRequestContext context,
                                                   MpprRequest mpprRequest) {
        val parserRequest = new QueryParserRequest(context.getDmlSubQuery(), context.getLogicalSchema());
        return columnMetadataService.getColumnMetadata(parserRequest)
                .map(metadata -> {
                    mpprRequest.setMetadata(metadata);
                    return mpprRequest;
                });
    }

    @Override
    public ExternalTableLocationType getDownloadType() {
        return ExternalTableLocationType.KAFKA;
    }
}
