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

import io.arenadata.dtm.async.AsyncUtils;
import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.PreparedQueryKey;
import io.arenadata.dtm.common.cache.PreparedQueryValue;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlSelectExt;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.dto.LlrRequestContext;
import io.arenadata.dtm.query.execution.core.dml.factory.LlrRequestContextFactory;
import io.arenadata.dtm.query.execution.core.dml.service.*;
import io.arenadata.dtm.query.execution.core.dml.service.view.ViewReplacerService;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsService;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.query.exception.QueriedEntityIsMissingException;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
public class LlrDmlExecutor implements DmlExecutor<QueryResult> {

    private final DataSourcePluginService dataSourcePluginService;
    private final AcceptableSourceTypesDefinitionService acceptableSourceTypesService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final ViewReplacerService viewReplacerService;
    private final InformationSchemaExecutor infoSchemaExecutor;
    private final InformationSchemaDefinitionService infoSchemaDefService;
    private final MetricsService<RequestMetrics> metricsService;
    private final QueryTemplateExtractor templateExtractor;
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService;
    private final CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService;
    private final LlrRequestContextFactory llrRequestContextFactory;
    private final SelectCategoryQualifier selectCategoryQualifier;
    private final SuitablePluginSelector suitablePluginSelector;
    private final SqlDialect sqlDialect;
    private final SqlParametersTypeExtractor parametersTypeExtractor;

    @Autowired
    public LlrDmlExecutor(DataSourcePluginService dataSourcePluginService,
                          AcceptableSourceTypesDefinitionService acceptableSourceTypesService,
                          DeltaQueryPreprocessor deltaQueryPreprocessor,
                          ViewReplacerService viewReplacerService,
                          InformationSchemaExecutor infoSchemaExecutor,
                          InformationSchemaDefinitionService infoSchemaDefService,
                          MetricsService<RequestMetrics> metricsService,
                          @Qualifier("coreQueryTmplateExtractor") QueryTemplateExtractor templateExtractor,
                          @Qualifier("coreQueryTemplateCacheService") CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService,
                          @Qualifier("corePreparedQueryCacheService") CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService,
                          LlrRequestContextFactory llrRequestContextFactory,
                          SelectCategoryQualifier selectCategoryQualifier,
                          SuitablePluginSelector suitablePluginSelector,
                          @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                          SqlParametersTypeExtractor parametersTypeExtractor) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.acceptableSourceTypesService = acceptableSourceTypesService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.viewReplacerService = viewReplacerService;
        this.infoSchemaExecutor = infoSchemaExecutor;
        this.infoSchemaDefService = infoSchemaDefService;
        this.metricsService = metricsService;
        this.templateExtractor = templateExtractor;
        this.queryCacheService = queryCacheService;
        this.preparedQueryCacheService = preparedQueryCacheService;
        this.llrRequestContextFactory = llrRequestContextFactory;
        this.selectCategoryQualifier = selectCategoryQualifier;
        this.suitablePluginSelector = suitablePluginSelector;
        this.sqlDialect = sqlDialect;
        this.parametersTypeExtractor = parametersTypeExtractor;
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        val queryRequest = context.getRequest().getQueryRequest();
        val sqlNode = context.getSqlNode();
        if (queryRequest.isPrepare()) {
            return prepareQuery(context, queryRequest);
        } else {
            return AsyncUtils.measureMs(replaceViews(queryRequest, sqlNode),
                            duration -> log.debug("Replaced views in request [{}] in [{}]ms", queryRequest.getSql(), duration))
                    .map(sqlNodeWithoutViews -> {
                        queryRequest.setSql(sqlNodeWithoutViews.toSqlString(sqlDialect).toString());
                        return sqlNodeWithoutViews;
                    })
                    .compose(sqlNodeWithoutViews -> defineQueryAndExecute(sqlNodeWithoutViews, context));
        }
    }

    private Future<QueryResult> prepareQuery(DmlRequestContext context, QueryRequest queryRequest) {
        val sql = queryRequest.getSql();
        val originalQuery = context.getSqlNode();
        log.debug("Prepare sql query [{}]", sql);
        preparedQueryCacheService.put(new PreparedQueryKey(sql), new PreparedQueryValue(originalQuery));
        return replaceViews(queryRequest, originalQuery)
                .map(sqlNodeWithoutViews -> {
                    val originalNode = context.getSqlNode();
                    context.setSqlNode(sqlNodeWithoutViews);
                    return originalNode;
                })
                .compose(withoutViewsQuery -> createLlrRequestContext(Optional.empty(),
                        withoutViewsQuery,
                        originalQuery,
                        context))
                .compose(this::initQuerySourceTypeAndUpdateQueryCacheIfNeeded)
                .compose(llrRequestContext -> deltaQueryPreprocessor.process(context.getSqlNode())
                        .map(dr -> {
                            llrRequestContext.setDeltaInformations(dr.getDeltaInformations());
                            return llrRequestContext;
                        }))
                .compose(llrRequestContext -> dataSourcePluginService.prepareLlr(defineSourceType(llrRequestContext),
                        llrRequestContext.getDmlRequestContext().getMetrics(),
                        createLlrRequest(llrRequestContext)))
                .map(v -> QueryResult.emptyResult());
    }

    private Future<SqlNode> replaceViews(QueryRequest queryRequest,
                                         SqlNode sqlNode) {
        return viewReplacerService.replace(sqlNode, queryRequest.getDatamartMnemonic())
                .map(sqlNodeWithoutViews -> {
                    queryRequest.setSql(sqlNodeWithoutViews.toSqlString(sqlDialect).toString());
                    return sqlNodeWithoutViews;
                });
    }

    private Future<QueryResult> defineQueryAndExecute(SqlNode withoutViewsQuery, DmlRequestContext context) {
        log.debug("Execute sql query [{}]", context.getRequest().getQueryRequest());
        val originalQuery = context.getSqlNode();
        val withSnapshots = SqlNodeUtil.copy(withoutViewsQuery);
        val estimate = figureOutEstimate(context.getSqlNode());
        context.setSqlNode(withoutViewsQuery);

        return AsyncUtils.measureMs(deltaQueryPreprocessor.process(context.getSqlNode()),
                        duration -> log.debug("Extracted deltas from query [{}] in [{}]ms",
                                context.getRequest().getQueryRequest().getSql(), duration))
                .compose(deltaResponse -> {
                    if (infoSchemaDefService.isInformationSchemaRequest(deltaResponse.getDeltaInformations())) {
                        return executeInformationSchemaRequest(context, originalQuery, deltaResponse);
                    } else {
                        return executeLlrRequest(context,
                                withoutViewsQuery,
                                withSnapshots,
                                deltaResponse,
                                estimate);
                    }
                });
    }

    private boolean figureOutEstimate(SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelectExt) {
            return ((SqlSelectExt) sqlNode).isEstimate();
        }

        if (sqlNode instanceof LimitableSqlOrderBy) {
            return ((LimitableSqlOrderBy) sqlNode).isEstimate();
        }

        return false;
    }

    private Future<QueryResult> executeInformationSchemaRequest(DmlRequestContext context,
                                                                SqlNode originalQuery,
                                                                DeltaQueryPreprocessorResponse deltaResponse) {
        return initLlrRequestContext(context, deltaResponse)
                .compose(llrRequestContext -> checkAccessAndExecute(llrRequestContext, originalQuery));
    }

    private Future<QueryResult> checkAccessAndExecute(LlrRequestContext llrRequestContext, SqlNode originalQuery) {
        return Future.future(p -> metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.LLR,
                        llrRequestContext.getDmlRequestContext().getMetrics())
                .compose(v -> infoSchemaDefService.checkAccessToSystemLogicalTables(originalQuery))
                .compose(v -> infoSchemaExecutor.execute(llrRequestContext.getSourceRequest()))
                .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.LLR,
                        llrRequestContext.getDmlRequestContext().getMetrics(),
                        p))
        );
    }

    private Future<LlrRequestContext> initLlrRequestContext(DmlRequestContext context,
                                                            DeltaQueryPreprocessorResponse deltaResponse) {
        return llrRequestContextFactory.create(deltaResponse, context)
                .map(llrRequestContext -> {
                    llrRequestContext.getSourceRequest().setQuery(context.getSqlNode());
                    return llrRequestContext;
                });
    }

    private Future<QueryResult> executeLlrRequest(DmlRequestContext context,
                                                  SqlNode withoutViewsQuery,
                                                  SqlNode originalQuery,
                                                  DeltaQueryPreprocessorResponse deltaResponse,
                                                  boolean estimate) {
        return createLlrRequestContext(Optional.of(deltaResponse), withoutViewsQuery, originalQuery, context)
                .compose(llrContext -> AsyncUtils.measureMs(initQuerySourceTypeAndUpdateQueryCacheIfNeeded(llrContext),
                        duration -> log.debug("Initialized query type for query [{}] in [{}]ms",
                                llrContext.getQueryTemplateValue().getSql(), duration)))
                .compose(llrRequestContext -> {
                    if (!estimate) {
                        return dataSourcePluginService.llr(defineSourceType(llrRequestContext),
                                llrRequestContext.getDmlRequestContext().getMetrics(),
                                createLlrRequest(llrRequestContext));
                    } else {
                        return dataSourcePluginService.llrEstimate(defineSourceType(llrRequestContext),
                                llrRequestContext.getDmlRequestContext().getMetrics(),
                                createLlrRequest(llrRequestContext));
                    }
                });
    }

    private Future<LlrRequestContext> createLlrRequestContext(Optional<DeltaQueryPreprocessorResponse> deltaResponseOpt,
                                                              SqlNode withoutViewsQuery,
                                                              SqlNode originalQuery,
                                                              DmlRequestContext context) {
        val templateResult = createQueryTemplateResult(withoutViewsQuery);
        Optional<SourceQueryTemplateValue> sourceQueryTemplateValueOpt =
                Optional.ofNullable(queryCacheService.get(QueryTemplateKey.builder()
                        .sourceQueryTemplate(templateResult.getTemplate())
                        .build()));
        if (sourceQueryTemplateValueOpt.isPresent()) {
            val queryTemplateValue = sourceQueryTemplateValueOpt.get();
            log.debug("Found query template cache value by key [{}]", templateResult.getTemplate());
            return deltaQueryPreprocessor.process(templateResult.getTemplateNode())
                    .compose(delta -> {
                        context.setSqlNode(templateExtractor
                                .extract(delta.getSqlNode())
                                .getTemplateNode());
                        return llrRequestContextFactory.create(context, queryTemplateValue)
                                .map(llrRequestContext -> {
                                    llrRequestContext.getSourceRequest().setQueryTemplate(templateResult);
                                    llrRequestContext.setOriginalQuery(originalQuery);
                                    deltaResponseOpt.ifPresent(d -> llrRequestContext.setDeltaInformations(d.getDeltaInformations()));
                                    return llrRequestContext;
                                });
                    });
        } else {
            if (deltaResponseOpt.isPresent()) {
                val deltaQueryPreprocessorResponse = deltaResponseOpt.get();
                SqlNode templateNode = templateExtractor.extract(deltaQueryPreprocessorResponse.getSqlNode()).getTemplateNode();
                context.setSqlNode(templateNode);
                return llrRequestContextFactory.create(deltaQueryPreprocessorResponse, context)
                        .map(llrRequestContext -> {
                            llrRequestContext.getSourceRequest().setQueryTemplate(templateResult);
                            llrRequestContext.setOriginalQuery(originalQuery);
                            return llrRequestContext;
                        })
                        .compose(this::cacheQueryTemplateValue);
            } else {
                SqlNode templateNode = templateExtractor.extract(context.getSqlNode()).getTemplateNode();
                context.setSqlNode(templateNode);
                return llrRequestContextFactory.create(context)
                        .map(llrRequestContext -> {
                            llrRequestContext.getSourceRequest().setQueryTemplate(templateResult);
                            llrRequestContext.setOriginalQuery(originalQuery);
                            return llrRequestContext;
                        })
                        .compose(this::cacheQueryTemplateValue);
            }
        }
    }

    private Future<LlrRequestContext> initQuerySourceTypeAndUpdateQueryCacheIfNeeded(LlrRequestContext llrContext) {
        if (llrContext.getSourceRequest().getSourceType() == null
                && llrContext.getQueryTemplateValue().getMostSuitablePlugin() == null) {
            val selectCategory = selectCategoryQualifier.qualify(llrContext.getQueryTemplateValue().getLogicalSchema(),
                    llrContext.getDmlRequestContext().getSqlNode());
            val sourceType = suitablePluginSelector.selectByCategory(selectCategory,
                    llrContext.getQueryTemplateValue().getAvailableSourceTypes());
            log.debug("Defined category [{}] for sql query [{}]", selectCategory,
                    llrContext.getDmlRequestContext().getRequest().getQueryRequest().getSql());
            llrContext.getQueryTemplateValue().setSelectCategory(selectCategory);
            llrContext.getQueryTemplateValue().setMostSuitablePlugin(sourceType.orElse(null));
            return queryCacheService.put(QueryTemplateKey.builder()
                                    .sourceQueryTemplate(llrContext.getSourceRequest().getQueryTemplate().getTemplate())
                                    .logicalSchema(llrContext.getSourceRequest().getLogicalSchema())
                                    .build(),
                            llrContext.getQueryTemplateValue())
                    .map(v -> llrContext);
        } else if (llrContext.getSourceRequest().getSourceType() != null
                && !llrContext.getQueryTemplateValue().getAvailableSourceTypes()
                .contains(llrContext.getSourceRequest().getSourceType())) {
            return Future.failedFuture(new QueriedEntityIsMissingException(llrContext.getSourceRequest().getSourceType()));
        } else {
            return Future.succeededFuture(llrContext);
        }
    }

    private QueryTemplateResult createQueryTemplateResult(SqlNode sqlNode) {
        val copySqlNode = SqlNodeUtil.copy(sqlNode);
        return templateExtractor.extract(copySqlNode);
    }

    private Future<LlrRequestContext> cacheQueryTemplateValue(LlrRequestContext llrRequestContext) {
        val newQueryTemplateKey = QueryTemplateKey.builder().build();
        val newQueryTemplateValue = SourceQueryTemplateValue.builder().build();
        llrRequestContext.setQueryTemplateValue(newQueryTemplateValue);
        initQueryTemplate(llrRequestContext, newQueryTemplateKey, newQueryTemplateValue);
        return acceptableSourceTypesService.define(llrRequestContext.getSourceRequest())
                .map(sourceTypes -> {
                    newQueryTemplateValue.setAvailableSourceTypes(sourceTypes);
                    return sourceTypes;
                })
                .compose(v -> queryCacheService.put(QueryTemplateKey.builder()
                                .sourceQueryTemplate(llrRequestContext.getSourceRequest().getQueryTemplate().getTemplate())
                                .logicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema())
                                .build(),
                        llrRequestContext.getQueryTemplateValue()))
                .map(v -> llrRequestContext);
    }

    private void initQueryTemplate(LlrRequestContext llrRequestContext,
                                   QueryTemplateKey newQueryTemplateKey,
                                   SourceQueryTemplateValue newQueryTemplateValue) {
        newQueryTemplateKey.setSourceQueryTemplate(llrRequestContext.getSourceRequest().getQueryTemplate().getTemplate());
        newQueryTemplateKey.setLogicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema());
        newQueryTemplateValue.setMetadata(llrRequestContext.getSourceRequest().getMetadata());
        newQueryTemplateValue.setLogicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema());
        newQueryTemplateValue.setSql(llrRequestContext.getSourceRequest().getQueryRequest().getSql());
        newQueryTemplateValue.setParameterTypes(parametersTypeExtractor.extract(llrRequestContext.getRelNode().rel));
    }

    private SourceType defineSourceType(LlrRequestContext llrRequestContext) {
        SourceType sourceType = llrRequestContext.getSourceRequest().getSourceType() == null ?
                llrRequestContext.getQueryTemplateValue().getMostSuitablePlugin() :
                llrRequestContext.getSourceRequest().getSourceType();
        log.debug("Defined source type [{}] for query [{}]",
                sourceType,
                llrRequestContext.getDmlRequestContext().getRequest().getQueryRequest().getSql());
        return sourceType;
    }

    private LlrRequest createLlrRequest(LlrRequestContext context) {
        QueryRequest queryRequest = context.getDmlRequestContext().getRequest().getQueryRequest();
        return LlrRequest.builder()
                .sourceQueryTemplateResult(context.getSourceRequest().getQueryTemplate())
                .parameters(context.getSourceRequest().getQueryRequest().getParameters())
                .parameterTypes(context.getQueryTemplateValue().getParameterTypes())
                .withoutViewsQuery(context.getDmlRequestContext().getSqlNode())
                .schema(context.getSourceRequest().getLogicalSchema())
                .envName(context.getDmlRequestContext().getEnvName())
                .datamartMnemonic(queryRequest.getDatamartMnemonic())
                .deltaInformations(context.getDeltaInformations())
                .metadata(context.getSourceRequest().getMetadata())
                .deltaInformations(context.getDeltaInformations())
                .originalQuery(context.getOriginalQuery())
                .requestId(queryRequest.getRequestId())
                .build();
    }

    @Override
    public DmlType getType() {
        return DmlType.LLR;
    }

}
