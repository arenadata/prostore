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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.enrichment;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.calcite.core.rel2sql.NullNotCastableRelToSqlConverter;
import io.arenadata.dtm.query.execution.plugin.adb.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryGenerator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class AdbQueryGeneratorImpl implements QueryGenerator {

    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;

    public AdbQueryGeneratorImpl(QueryExtendService queryExtendService,
                                 @Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public void mutateQuery(RelRoot relNode,
                            List<DeltaInformation> deltaInformations,
                            CalciteContext calciteContext,
                            Handler<AsyncResult<String>> handler) {
        if (deltaInformations.isEmpty()) {
            log.warn("Deltas list cannot be empty");
        }
        try {
            val generatorContext = getContext(relNode, deltaInformations, calciteContext);
            val extendedQuery = queryExtendService.extendQuery(generatorContext);
            val planAfter = calciteContext.getPlanner().transform(0,
                    extendedQuery.getTraitSet().replace(EnumerableConvention.INSTANCE),
                    extendedQuery);
            val sqlNodeResult = new NullNotCastableRelToSqlConverter(sqlDialect).visitChild(0, planAfter).asStatement();
            val queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql()).replaceAll("\n", " ");
            log.debug("sql = " + queryResult);
            handler.handle(Future.succeededFuture(queryResult));
        } catch (Exception e) {
            log.error("Conversion execution error", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext) {
        return new QueryGeneratorContext(
                deltaInformations.iterator(),
                calciteContext.getRelBuilder(),
                true,
                relNode);
    }
}
