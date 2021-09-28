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
package io.arenadata.dtm.query.execution.core.dml.factory;

import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.dto.LlrRequestContext;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LlrRequestContextFactory {

    private final LogicalSchemaProvider logicalSchemaProvider;
    private final ColumnMetadataService columnMetadataService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final QueryParserService parserService;

    @Autowired
    public LlrRequestContextFactory(LogicalSchemaProvider logicalSchemaProvider,
                                    ColumnMetadataService columnMetadataService,
                                    DeltaQueryPreprocessor deltaQueryPreprocessor,
                                    CoreCalciteDMLQueryParserService parserService) {
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.parserService = parserService;
    }

    public Future<LlrRequestContext> create(DmlRequestContext context) {
        LlrRequestContext llrContext = createLlrRequestContext(context);
        return initDeltaInformations(llrContext)
                .compose(v -> initLlrContext(llrContext));
    }

    public Future<LlrRequestContext> create(DeltaQueryPreprocessorResponse deltaResponse, DmlRequestContext context) {
        LlrRequestContext llrContext = createLlrRequestContext(context);
        llrContext.setDeltaInformations(deltaResponse.getDeltaInformations());
        llrContext.getDmlRequestContext().setSqlNode(deltaResponse.getSqlNode());
        return initLlrContext(llrContext);
    }

    public Future<LlrRequestContext> create(DmlRequestContext context, SourceQueryTemplateValue queryTemplateValue) {
        LlrRequestContext llrContext = createLlrRequestContext(context);
        llrContext.getSourceRequest().setMetadata(queryTemplateValue.getMetadata());
        llrContext.getSourceRequest().setLogicalSchema(queryTemplateValue.getLogicalSchema());
        llrContext.getSourceRequest().getQueryRequest().setSql(queryTemplateValue.getSql());
        llrContext.setQueryTemplateValue(queryTemplateValue);
        return Future.succeededFuture(llrContext);
    }

    private LlrRequestContext createLlrRequestContext(DmlRequestContext context) {
        val sourceRequest = new QuerySourceRequest(context.getRequest().getQueryRequest(),
                context.getSqlNode(),
                context.getSourceType());
        return LlrRequestContext.builder()
                .sourceRequest(sourceRequest)
                .dmlRequestContext(context)
                .build();
    }

    private Future<LlrRequestContext> initDeltaInformations(LlrRequestContext llrContext) {
        return deltaQueryPreprocessor.process(llrContext.getDmlRequestContext().getSqlNode())
                .map(preprocessorResponse -> {
                    llrContext.setDeltaInformations(preprocessorResponse.getDeltaInformations());
                    llrContext.getDmlRequestContext().setSqlNode(preprocessorResponse.getSqlNode());
                    return llrContext;
                });
    }

    private Future<LlrRequestContext> initLlrContext(LlrRequestContext llrContext) {
        return logicalSchemaProvider.getSchemaFromQuery(
                llrContext.getDmlRequestContext().getSqlNode(),
                llrContext.getDmlRequestContext().getRequest().getQueryRequest().getDatamartMnemonic())
                .map(schema -> {
                    llrContext.getSourceRequest().setLogicalSchema(schema);
                    return llrContext;
                })
                .compose(v -> parserService.parse(new QueryParserRequest(llrContext.getDmlRequestContext().getSqlNode(),
                        llrContext.getSourceRequest().getLogicalSchema())))
                .map(response -> {
                    llrContext.setRelNode(response.getRelNode());
                    return response;
                })
                .compose(response -> columnMetadataService.getColumnMetadata(response.getRelNode()))
                .map(metadata -> {
                    llrContext.getSourceRequest().setMetadata(metadata);
                    return llrContext;
                });
    }
}
