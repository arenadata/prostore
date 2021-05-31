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

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlAction;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.service.LogicViewReplacer;
import io.arenadata.dtm.query.execution.core.edml.mppr.service.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.core.edml.service.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.edml.dto.EdmlAction.DOWNLOAD;

@Service
@Slf4j
public class DownloadExternalTableExecutor implements EdmlExecutor {

    private final LogicalSchemaProvider logicalSchemaProvider;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final Map<ExternalTableLocationType, EdmlDownloadExecutor> executors;
    private final LogicViewReplacer logicViewReplacer;

    @Autowired
    public DownloadExternalTableExecutor(LogicalSchemaProvider logicalSchemaProvider,
                                         DeltaQueryPreprocessor deltaQueryPreprocessor,
                                         List<EdmlDownloadExecutor> downloadExecutors,
                                         LogicViewReplacer logicViewReplacer) {
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.executors = downloadExecutors.stream().collect(Collectors.toMap(EdmlDownloadExecutor::getDownloadType, it -> it));
        this.logicViewReplacer = logicViewReplacer;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return initDMLSubquery(context)
                .compose(v -> replaceView(context))
                .compose(v -> initDeltaInformation(context))
                .compose(v -> initLogicalSchema(context))
                .compose(v -> executeInternal(context));
    }

    private Future<Void> initDMLSubquery(EdmlRequestContext context) {
        return Future.future(promise -> {
            context.setDmlSubQuery(((SqlInsert) context.getSqlNode()).getSource());
            promise.complete();
        });
    }

    private Future<SqlNode> replaceView(EdmlRequestContext context) {
        val datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return logicViewReplacer.replace(context.getDmlSubQuery(), datamartMnemonic)
                .map(result -> {
                    context.setDmlSubQuery(result);
                    return result;
                });
    }

    private Future<Void> initLogicalSchema(EdmlRequestContext context) {
        return Future.future(promise ->
                logicalSchemaProvider.getSchemaFromDeltaInformations(context.getDeltaInformations(),
                        context.getRequest().getQueryRequest().getDatamartMnemonic())
                        .onSuccess(schema -> {
                            context.setLogicalSchema(schema);
                            promise.complete();
                        })
                        .onFailure(promise::fail));
    }

    private Future<EdmlRequestContext> initDeltaInformation(EdmlRequestContext context) {
        return Future.future(promise ->
                deltaQueryPreprocessor.process(context.getDmlSubQuery())
                        .onSuccess(result -> {
                            context.setDeltaInformations(result.getDeltaInformations());
                            context.setDmlSubQuery(result.getSqlNode());
                            promise.complete(context);
                        })
                        .onFailure(promise::fail));
    }

    private Future<QueryResult> executeInternal(EdmlRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            val destination = context.getDestinationEntity();
            if (ExternalTableLocationType.KAFKA == destination.getExternalTableLocationType()) {
                executors.get(destination.getExternalTableLocationType()).execute(context)
                        .onSuccess(queryResult -> {
                            log.debug("Mppr into table [{}] for dml query [{}] finished successfully",
                                    destination.getName(), context.getDmlSubQuery());
                            promise.complete(queryResult);
                        })
                        .onFailure(fail -> promise.fail(new DtmException(
                                String.format("Error executing mppr into table [%s] for dml query [%s]: %s",
                                        destination.getName(),
                                        context.getDmlSubQuery(),
                                        fail == null ? "" : fail.getMessage()),
                                fail)));
            } else {
                promise.fail(new DtmException("Other types of upload are not yet implemented!"));
            }
        });
    }

    @Override
    public EdmlAction getAction() {
        return DOWNLOAD;
    }
}
