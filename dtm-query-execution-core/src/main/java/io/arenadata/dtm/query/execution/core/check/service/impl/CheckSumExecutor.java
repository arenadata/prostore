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

import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckSum;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaIsEmptyException;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.check.dto.CheckSumRequestContext;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.check.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.core.check.service.CheckExecutor;
import io.arenadata.dtm.query.execution.core.check.service.CheckSumTableService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service("checkSumExecutor")
public class CheckSumExecutor implements CheckExecutor {

    private final DeltaServiceDao deltaServiceDao;
    private final EntityDao entityDao;
    private final CheckSumTableService checkSumTableService;
    private final CheckQueryResultFactory queryResultFactory;

    @Autowired
    public CheckSumExecutor(DeltaServiceDao deltaServiceDao,
                            EntityDao entityDao,
                            CheckSumTableService checkSumTableService,
                            CheckQueryResultFactory queryResultFactory) {
        this.deltaServiceDao = deltaServiceDao;
        this.entityDao = entityDao;
        this.checkSumTableService = checkSumTableService;
        this.queryResultFactory = queryResultFactory;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        return Future.future(promise -> {
            SqlCheckSum sqlCheckSum = (SqlCheckSum) context.getSqlNode();
            val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
            val deltaNum = sqlCheckSum.getDeltaNum();
            val table = Optional.ofNullable(sqlCheckSum.getTable());
            val columns = sqlCheckSum.getColumns();
            val checkContext = CheckSumRequestContext.builder()
                    .checkContext(context)
                    .deltaNum(deltaNum)
                    .datamart(datamart)
                    .columns(columns)
                    .build();
            deltaServiceDao.getDeltaHot(datamart)
                    .compose(hotDelta -> {
                        if (hotDelta == null || hotDelta.getDeltaNum() != deltaNum) {
                            return deltaServiceDao.getDeltaByNum(datamart, deltaNum)
                                    .compose(okDelta -> calculateCheckSum(table, checkContext, okDelta.getCnFrom(), okDelta.getCnTo()));
                        } else {
                            return calculateCheckSum(table, checkContext, hotDelta.getCnFrom(), hotDelta.getCnTo());
                        }
                    })
                    .onSuccess(sum -> promise.complete(createQueryResult(sum)))
                    .onFailure(promise::fail);
        });
    }

    private Future<Long> calculateCheckSum(Optional<String> table, CheckSumRequestContext checkContext, long cnFrom, Long cnTo) {
        if (cnTo == null) {
            return Future.failedFuture(new DeltaIsEmptyException(checkContext.getDeltaNum()));
        }
        checkContext.setCnFrom(cnFrom);
        checkContext.setCnTo(cnTo);
        if (table.isPresent()) {
            return entityDao.getEntity(checkContext.getDatamart(), table.get())
                    .map(e -> {
                        if (e.getEntityType() != EntityType.TABLE) {
                            throw new EntityNotExistsException(e.getName());
                        } else {
                            checkContext.setEntity(e);
                            return e;
                        }
                    })
                    .compose(entity -> checkSumTableService.calcCheckSumTable(checkContext));
        } else {
            return checkSumTableService.calcCheckSumForAllTables(checkContext);
        }
    }

    private QueryResult createQueryResult(Long sum) {
        return queryResultFactory.create(sum == null ? null : sum.toString());
    }

    @Override
    public CheckType getType() {
        return CheckType.SUM;
    }
}
