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
package io.arenadata.dtm.query.execution.plugin.adp.enrichment.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.rel2sql.NullNotCastableRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.util.RelNodeUtil;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryGenerator;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("adpQueryGenerator")
@Slf4j
public class AdpQueryGenerator implements QueryGenerator {

    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;

    @Autowired
    public AdpQueryGenerator(@Qualifier("adpDmlExtendService") QueryExtendService queryExtendService,
                             @Qualifier("adpSqlDialect") SqlDialect sqlDialect) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<String> mutateQuery(RelRoot relNode,
                                      List<DeltaInformation> deltaInformations,
                                      CalciteContext calciteContext,
                                      EnrichQueryRequest enrichQueryRequest) {
        if (deltaInformations.isEmpty()) {
            log.warn("Deltas list cannot be empty");
        }
        return Future.future(promise -> {
            val generatorContext = getContext(relNode, deltaInformations, calciteContext);
            val extendedQuery = queryExtendService.extendQuery(generatorContext);
            RelNode resultRelNode = null;
            if (RelNodeUtil.isNeedToTrimSortColumns(relNode, extendedQuery)) {
                resultRelNode = RelNodeUtil.trimUnusedSortColumn(calciteContext.getRelBuilder(),
                        extendedQuery,
                        relNode.validatedRowType);
            } else {
                try {
                    resultRelNode = calciteContext.getPlanner()
                            .transform(0, extendedQuery.getTraitSet().replace(EnumerableConvention.INSTANCE),
                                    extendedQuery);
                } catch (Exception e) {
                    promise.fail(new DtmException("Error in converting rel node", e));
                    return;
                }
            }
            val sqlNodeResult = new NullNotCastableRelToSqlConverter(sqlDialect).visitChild(0, resultRelNode).asStatement();
            val queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql()).replaceAll("\r\n|\r|\n", " ");
            log.debug("sql = " + queryResult);
            promise.complete(queryResult);
        });
    }

    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext) {
        return new QueryGeneratorContext(
                deltaInformations.iterator(),
                calciteContext.getRelBuilder(),
                relNode,
                true);
    }
}
