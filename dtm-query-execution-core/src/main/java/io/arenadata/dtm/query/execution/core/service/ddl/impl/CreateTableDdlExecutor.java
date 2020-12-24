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

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Component
public class CreateTableDdlExecutor extends QueryResultDdlExecutor {

    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public CreateTableDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                  ServiceDbFacade serviceDbFacade,
                                  MetadataCalciteGenerator metadataCalciteGenerator,
                                  DataSourcePluginService dataSourcePluginService) {
        super(metadataExecutor, serviceDbFacade);
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            val schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
            context.getRequest().getQueryRequest().setDatamartMnemonic(schema);
            context.setDdlType(DdlType.CREATE_TABLE);
            SqlCreateTable sqlCreate = (SqlCreateTable) context.getQuery();
            val entity = metadataCalciteGenerator.generateTableMetadata(sqlCreate);
            entity.setEntityType(EntityType.TABLE);
            Set<SourceType> requestDestination = ((SqlCreateTable) context.getQuery()).getDestination();
            Set<SourceType> destination = Optional.ofNullable(requestDestination)
                    .orElse(dataSourcePluginService.getSourceTypes());
            entity.setDestination(destination);
            checkRequiredKeys(entity.getFields());
            context.getRequest().setEntity(entity);
            context.setDatamartName(schema);
            datamartDao.existsDatamart(schema)
                .compose(isExistsDatamart -> isExistsDatamart ?
                    entityDao.existsEntity(schema, entity.getName()) : getNotExistsDatamartFuture(schema))
                .onSuccess(isExistsEntity -> createTableIfNotExists(context, isExistsEntity)
                    .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail))))
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error creating table by query request: {}!", context.getRequest().getQueryRequest(), e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Boolean> getNotExistsDatamartFuture(String schema) {
        return Future.failedFuture(new DatamartNotExistsException(schema));
    }

    private void checkRequiredKeys(List<EntityField> fields) {
        val notExistsKeys = new ArrayList<String>();
        val notExistsPrimaryKeys = fields.stream()
            .noneMatch(f -> f.getPrimaryOrder() != null);
        if (notExistsPrimaryKeys) {
            notExistsKeys.add("primary key(s)");
        }

        val notExistsShardingKey = fields.stream()
            .noneMatch(f -> f.getShardingOrder() != null);
        if (notExistsShardingKey) {
            notExistsKeys.add("sharding key(s)");
        }

        if (!notExistsKeys.isEmpty()) {
            throw new IllegalArgumentException(
                "Primary keys and Sharding keys are required. The following keys do not exist: " + String.join(",", notExistsKeys)
            );
        }
    }

    private Future<Void> createTableIfNotExists(DdlRequestContext context,
                                                Boolean isTableExists) {
        if (isTableExists) {
            final RuntimeException existsException =
                new RuntimeException(String.format("Table [%s] is already exists in datamart [%s]!",
                    context.getRequest().getEntity().getName(),
                    context.getRequest().getEntity().getSchema()));
            log.error("Error creating table [{}] in datamart [{}]!",
                context.getRequest().getEntity().getName(),
                context.getRequest().getEntity().getSchema(),
                existsException);
            return Future.failedFuture(existsException);
        } else {
            return createTable(context);
        }
    }

    private Future<Void> createTable(DdlRequestContext context) {
        //creating tables in data sources through plugins
        return Future.future((Promise<Void> promise) -> {
            metadataExecutor.execute(context, ar -> {
                if (ar.succeeded()) {
                    entityDao.createEntity(context.getRequest().getEntity())
                        .onSuccess(ar2 -> {
                            log.debug("Table [{}] in datamart [{}] successfully created",
                                context.getRequest().getEntity().getName(),
                                context.getDatamartName());
                            promise.complete();
                        })
                        .onFailure(fail -> {
                            log.error("Error creating table [{}] in datamart [{}]!",
                                context.getRequest().getEntity().getName(),
                                context.getDatamartName(), fail);
                            promise.fail(fail);
                        });
                } else {
                    log.error("Error creating table [{}], datamart [{}] in datasources!",
                        context.getRequest().getEntity().getName(),
                        context.getDatamartName(),
                        ar.cause());
                    promise.fail(ar.cause());
                }
            });
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }
}
