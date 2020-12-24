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
package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.service.RestoreStateService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Component
public class RestoreStateServiceImpl implements RestoreStateService {

    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final DeltaServiceDao deltaServiceDao;
    private final EdmlUploadFailedExecutor edmlUploadFailedExecutor;
    private final UploadExternalTableExecutor uploadExternalTableExecutor;
    private final DefinitionService<SqlNode> definitionService;
    private final String envName;
    private final DtmConfig dtmSettings;

    @Autowired
    public RestoreStateServiceImpl(ServiceDbFacade serviceDbFacade,
                                   EdmlUploadFailedExecutor edmlUploadFailedExecutor,
                                   UploadExternalTableExecutor uploadExternalTableExecutor,
                                   @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                   @Value("${core.env.name}") String envName,
                                   DtmConfig dtmSettings) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.edmlUploadFailedExecutor = edmlUploadFailedExecutor;
        this.uploadExternalTableExecutor = uploadExternalTableExecutor;
        this.definitionService = definitionService;
        this.envName = envName;
        this.dtmSettings = dtmSettings;
    }

    @Override
    public Future<Void> restoreState() {
        return datamartDao.getDatamarts()
                .compose(this::processDatamarts)
                .onSuccess(success -> log.info("State sucessfully restored"))
                .onFailure(err -> log.error("Error while trying to restore state", err));
    }

    private Future<Void> processDatamarts(List<String> datamarts) {
        return Future.future(p -> {
            CompositeFuture.join(datamarts.stream()
                    .map(this::getAndProcessOpertations)
                    .collect(Collectors.toList()))
                    .onSuccess(success -> p.complete())
                    .onFailure(p::fail);
        });
    }

    private Future<Void> getAndProcessOpertations(String datamart) {
        return deltaServiceDao.getDeltaWriteOperations(datamart)
                .compose(ops -> processOperations(datamart, ops));
    }

    private Future<Void> processOperations(String datamart, List<DeltaWriteOp> ops) {
        if (ops == null) {
            return Future.succeededFuture();
        }
        return Future.future(p -> {
            CompositeFuture.join(ops.stream()
                    .map(op -> getDestinationSourceEntities(datamart, op.getTableName(), op.getTableNameExt())
                            .compose(entities -> processWriteOperation(entities.get(0), entities.get(1), op)))
                    .collect(Collectors.toList()))
                    .onSuccess(success -> p.complete())
                    .onFailure(p::fail);
        });
    }

    private Future<List<Entity>> getDestinationSourceEntities(String datamart, String dest, String source) {
        return Future.future(p -> CompositeFuture.join(entityDao.getEntity(datamart, dest), entityDao.getEntity(datamart, source))
                .onSuccess(res -> p.complete(res.list()))
                .onFailure(p::fail));
    }

    private Future<Void> processWriteOperation(Entity dest, Entity source, DeltaWriteOp op) {
        Promise promise = Promise.promise();

        val queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setEnvName(envName);
        queryRequest.setSql(op.getQuery());
        val datamartRequest = new DatamartRequest(queryRequest);
        val sqlNode = definitionService.processingQuery(op.getQuery());
        val context = new EdmlRequestContext(
                RequestMetrics.builder()
                        .startTime(LocalDateTime.now(dtmSettings.getTimeZone()))
                        .requestId(queryRequest.getRequestId())
                        .status(RequestStatus.IN_PROCESS)
                        .isActive(true)
                        .build(),
                datamartRequest, (SqlInsert) sqlNode);
        context.setSysCn(op.getSysCn());
        context.setSourceEntity(source);
        context.setDestinationEntity(dest);

        if (op.getStatus() == 0) {
            uploadExternalTableExecutor.execute(context, promise);
            return promise.future();
        } else if (op.getStatus() == 2) {
            return edmlUploadFailedExecutor.execute(context);
        } else return Future.succeededFuture();
    }
}
