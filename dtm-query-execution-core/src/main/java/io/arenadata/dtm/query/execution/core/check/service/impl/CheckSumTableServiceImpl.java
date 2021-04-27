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
package io.arenadata.dtm.query.execution.core.check.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.check.dto.CheckSumRequestContext;
import io.arenadata.dtm.query.execution.core.check.exception.CheckSumException;
import io.arenadata.dtm.query.execution.core.check.service.CheckSumTableService;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.util.Pair;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CheckSumTableServiceImpl implements CheckSumTableService {

    public static final String HASH_SUM_SYS_CN_DELIMITER = ";";
    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;

    @Autowired
    public CheckSumTableServiceImpl(DataSourcePluginService dataSourcePluginService, EntityDao entityDao) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.entityDao = entityDao;
    }

    @Override
    public Future<Long> calcCheckSumTable(CheckSumRequestContext request) {
        return Future.future(promise -> {
            List<String> sysCnHashList = new ArrayList<>();
            long sysCnFrom = request.getCnFrom();
            long sysCnTo = request.getCnTo();

            calcCheckSumInDataSources(sysCnFrom, sysCnTo, sysCnHashList, request)
                    .onSuccess(result -> promise.complete(convertCheckSumsToLong(sysCnHashList)))
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> calcCheckSumInDataSources(long sysCnTo,
                                                   long sysCn,
                                                   List<String> sysCnHashList,
                                                   CheckSumRequestContext request) {
        if (sysCn < sysCnTo) {
            return Future.succeededFuture();
        } else {
            return Future.future(promise -> CompositeFuture.join(request.getEntity().getDestination().stream()
                    .map(sourceType -> calcCheckSumLoop(sysCn, sourceType, request))
                    .collect(Collectors.toList()))
                    .onSuccess(result -> {
                        List<Pair<SourceType, Long>> resultList = result.list();
                        long distinctCount = resultList.stream()
                                .map(Pair::getValue)
                                .distinct().count();
                        if (distinctCount == 1) {
                            Long hashSum = resultList.get(0).getValue();
                            sysCnHashList.add(hashSum.toString());
                            calcCheckSumInDataSources(sysCnTo, sysCn - 1, sysCnHashList, request)
                                    .onSuccess(promise::complete)
                                    .onFailure(promise::fail);
                        } else {
                            promise.fail(new CheckSumException(request.getEntity().getName()));
                        }
                    })
                    .onFailure(promise::fail));
        }
    }

    private Future<Pair<SourceType, Long>> calcCheckSumLoop(Long sysCn, SourceType sourceType, CheckSumRequestContext context) {
        return Future.future(promise -> dataSourcePluginService.checkDataByHashInt32(sourceType,
                context.getCheckContext().getMetrics(),
                new CheckDataByHashInt32Request(context.getEntity(),
                        sysCn,
                        getColumns(context),
                        context.getCheckContext().getEnvName(),
                        context.getCheckContext().getRequest().getQueryRequest().getRequestId(),
                        context.getDatamart()))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(new Pair<>(sourceType, ar.result()));
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public Long convertCheckSumsToLong(List<String> sysCnHashList) {
        val hashSum = String.join(HASH_SUM_SYS_CN_DELIMITER, sysCnHashList);
        val md5 = DigestUtils.md5Hex(hashSum).toLowerCase();
        long result = 0;
        int offset = 0;
        for (int i = 0; i < 8; i++) {
            result += Byte.toUnsignedLong(md5.getBytes()[i]) << offset;
            offset += 8;
        }
        return result;
    }

    private Set<String> getColumns(CheckSumRequestContext request) {
        return request.getColumns() == null ? request.getEntity().getFields().stream()
                .map(EntityField::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new)) : request.getColumns();
    }

    @Override
    public Future<Long> calcCheckSumForAllTables(CheckSumRequestContext request) {
        List<Long> entitiesHashList = new ArrayList<>();
        return entityDao.getEntityNamesByDatamart(request.getDatamart())
                .compose(entityNames -> getEntities(entityNames, request.getDatamart()))
                .compose(entities -> calcCheckSumLoop(entities.iterator(), entitiesHashList, request))
                .map(v ->
                        convertCheckSumsToLong(entitiesHashList.stream()
                                .map(Object::toString)
                                .collect(Collectors.toList())));
    }

    private Future<Long> calcCheckSumLoop(Iterator<Entity> iterator,
                                          List<Long> nwEntitiesHashList,
                                          CheckSumRequestContext request) {
        CheckSumRequestContext requestContext = request.copy();
        requestContext.setEntity(iterator.next());
        return calcCheckSumTable(requestContext)
                .onSuccess(nwEntitiesHashList::add)
                .compose(v -> iterator.hasNext() ? calcCheckSumLoop(iterator, nwEntitiesHashList, request) : Future.succeededFuture());
    }

    private Future<List<Entity>> getEntities(List<String> entityNames, String datamartMnemonic) {
        return Future.future(promise -> CompositeFuture.join(
                entityNames.stream()
                        .map(name -> entityDao.getEntity(datamartMnemonic, name))
                        .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Entity> entities = result.list();
                    promise.complete(entities.stream()
                            .filter(e -> e.getEntityType() == EntityType.TABLE)
                            .sorted(Comparator.comparing(Entity::getName))
                            .collect(Collectors.toList()));
                })
                .onFailure(promise::fail));
    }
}
