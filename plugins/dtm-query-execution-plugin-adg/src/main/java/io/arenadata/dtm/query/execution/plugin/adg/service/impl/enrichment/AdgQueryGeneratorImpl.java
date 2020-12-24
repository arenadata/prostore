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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.rel2sql.NullNotCastableRelToSqlConverter;
import io.arenadata.dtm.query.execution.plugin.adg.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryGenerator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("adgQueryGenerator")
@Slf4j
public class AdgQueryGeneratorImpl implements QueryGenerator {
    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;

    public AdgQueryGeneratorImpl(@Qualifier("adgCalciteDmlQueryExtendService") QueryExtendService queryExtendService,
                                 @Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public void mutateQuery(RelRoot relNode,
                            List<DeltaInformation> deltaInformations,
                            CalciteContext calciteContext,
                            QueryRequest queryRequest,
                            Handler<AsyncResult<String>> handler) {
        try {
            val generatorContext = getContext(relNode, deltaInformations, calciteContext, queryRequest);
            val extendedQuery = queryExtendService.extendQuery(generatorContext);
            RelNode planAfter = calciteContext.getPlanner().transform(0, extendedQuery.getTraitSet().replace(EnumerableConvention.INSTANCE), extendedQuery);
            SqlNode sqlNodeResult = new NullNotCastableRelToSqlConverter(sqlDialect).visitChild(0, planAfter).asStatement();
            String queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql()).replaceAll("\n", " ");
            log.debug("sql = " + queryResult);
            handler.handle(Future.succeededFuture(queryResult));
        } catch (Exception e) {
            log.error("Request conversion execution error", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext,
                                             QueryRequest queryRequest) {
        return new QueryGeneratorContext(
                deltaInformations.iterator(),
                queryRequest,
                calciteContext.getRelBuilder(),
                true,
                relNode);
    }
}
