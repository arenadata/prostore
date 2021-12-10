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
package io.arenadata.dtm.query.execution.core.dml.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlEstimateOnlyQuery;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.core.base.exception.upsert.UpsertSelectValidationException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.dto.PluginDeterminationRequest;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.dml.service.PluginDeterminationService;
import io.arenadata.dtm.query.execution.core.dml.service.SqlParametersTypeExtractor;
import io.arenadata.dtm.query.execution.core.dml.service.view.ViewReplacerService;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.dml.LlwUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.UpsertSelectRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class UpsertSelectExecutor extends UpsertExecutor<UpsertSelectRequest> {

    private final DataSourcePluginService pluginService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final QueryParserService queryParserService;
    private final ColumnMetadataService columnMetadataService;
    private final ViewReplacerService viewReplacerService;
    private final PluginDeterminationService pluginDeterminationService;
    private final QueryTemplateExtractor templateExtractor;
    private final SqlParametersTypeExtractor parametersTypeExtractor;

    public UpsertSelectExecutor(DataSourcePluginService pluginService,
                                ServiceDbFacade serviceDbFacade,
                                RestoreStateService restoreStateService,
                                LogicalSchemaProvider logicalSchemaProvider,
                                DeltaQueryPreprocessor deltaQueryPreprocessor,
                                @Qualifier("coreCalciteDMLQueryParserService") QueryParserService queryParserService,
                                ColumnMetadataService columnMetadataService,
                                ViewReplacerService viewReplacerService,
                                PluginDeterminationService pluginDeterminationService,
                                @Qualifier("coreQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                                SqlParametersTypeExtractor parametersTypeExtractor) {
        super(pluginService, serviceDbFacade, restoreStateService);
        this.pluginService = pluginService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.queryParserService = queryParserService;
        this.columnMetadataService = columnMetadataService;
        this.viewReplacerService = viewReplacerService;
        this.pluginDeterminationService = pluginDeterminationService;
        this.templateExtractor = templateExtractor;
        this.parametersTypeExtractor = parametersTypeExtractor;
    }

    @Override
    protected boolean isValidSource(SqlNode sqlInsert) {
        return LlwUtils.isSelectSqlNode(sqlInsert);
    }

    @Override
    protected Future<UpsertSelectRequest> buildRequest(DmlRequestContext context, Long sysCn, Entity entity) {
        return Future.future(promise -> {
            val sqlInsert = (SqlInsert) context.getSqlNode();
            val datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
            val source = sqlInsert.getSource();
            if (source instanceof SqlEstimateOnlyQuery && ((SqlEstimateOnlyQuery) source).isEstimate()) {
                throw UpsertSelectValidationException.estimateIsNotAllowed();
            }

            viewReplacerService.replace(source, datamartMnemonic)
                    .compose(expandedViewsQuery -> logicalSchemaProvider.getSchemaFromQuery(expandedViewsQuery, datamartMnemonic)
                            .compose(datamarts -> deltaQueryPreprocessor.process(expandedViewsQuery)
                                    .compose(deltaQueryPreprocessorResponse -> extractTemplate(deltaQueryPreprocessorResponse)
                                            .compose(templateResult -> queryParserService.parse(new QueryParserRequest(templateResult.getTemplateNode(), datamarts))
                                                    .compose(queryParserResponse -> validateQueryWithEntity(sqlInsert.getTargetColumnList(), entity, queryParserResponse.getRelNode())
                                                            .map(v -> {
                                                                val parametersTypes = parametersTypeExtractor.extract(queryParserResponse.getRelNode().rel);
                                                                val uuid = context.getRequest().getQueryRequest().getRequestId();
                                                                val env = context.getEnvName();
                                                                val params = context.getRequest().getQueryRequest().getParameters();
                                                                return new UpsertSelectRequest(uuid, env, datamartMnemonic, sysCn, entity, sqlInsert, params, datamarts,
                                                                        deltaQueryPreprocessorResponse.getDeltaInformations(), queryParserResponse.getSqlNode(), source, templateResult.getParams(), parametersTypes);
                                                            }))))))
                    .onComplete(promise);
        });
    }

    private Future<QueryTemplateResult> extractTemplate(DeltaQueryPreprocessorResponse deltaQueryPreprocessorResponse) {
        return Future.future(p -> p.complete(templateExtractor.extract(deltaQueryPreprocessorResponse.getSqlNode())));
    }

    private Future<Void> validateQueryWithEntity(SqlNodeList targetColumnList, Entity entity, RelRoot relRoot) {
        return columnMetadataService.getColumnMetadata(relRoot)
                .map(columnMetadata -> {
                    checkFieldsCount(targetColumnList, entity, columnMetadata);
                    return null;
                });
    }

    private void checkFieldsCount(SqlNodeList targetColumnList, Entity entity, List<ColumnMetadata> queryColumns) {
        int columnsCount = targetColumnList != null ? targetColumnList.size() : entity.getFields().size();

        if (columnsCount != queryColumns.size()) {
            throw UpsertSelectValidationException.columnCountConflict(entity.getName(), columnsCount, queryColumns.size());
        }
    }

    @Override
    protected Future<?> runOperation(DmlRequestContext context, UpsertSelectRequest upsertRequest) {
        val pluginDeterminationRequest = PluginDeterminationRequest.builder()
                .sqlNode(upsertRequest.getSourceQuery())
                .query(context.getRequest().getQueryRequest().getSql())
                .schema(upsertRequest.getDatamarts())
                .preferredSourceType(getPreferredSourceType(upsertRequest.getOriginalSourceQuery()))
                .build();
        return pluginDeterminationService.determine(pluginDeterminationRequest)
                .compose(pluginDeterminationResult -> {
                    val plugin = pluginDeterminationResult.getExecution();
                    if (!pluginService.hasSourceType(plugin)) {
                        return Future.failedFuture(new DtmException(String.format("Plugin [%s] is not enabled to run Upsert operation", plugin)));
                    }

                    return pluginService.upsert(plugin, context.getMetrics(), upsertRequest);
                });
    }

    private SourceType getPreferredSourceType(SqlNode sqlNode) {
        if (sqlNode instanceof SqlDataSourceTypeGetter) {
            return ((SqlDataSourceTypeGetter) sqlNode).getDatasourceType().getValue();
        }

        return null;
    }

    @Override
    public DmlType getType() {
        return DmlType.UPSERT_SELECT;
    }
}
