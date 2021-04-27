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
package io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.impl;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.rel2sql.NullNotCastableRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.util.RelNodeUtil;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.QueryGenerator;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("adgQueryGenerator")
@Slf4j
public class AdgQueryGeneratorImpl implements QueryGenerator {
    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;

    @Autowired
    public AdgQueryGeneratorImpl(@Qualifier("adgDmlQueryExtendService") QueryExtendService queryExtendService,
                                 @Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<String> mutateQuery(RelRoot relNode,
                                      List<DeltaInformation> deltaInformations,
                                      CalciteContext calciteContext,
                                      EnrichQueryRequest enrichQueryRequest) {
        return Future.future(promise -> {
            val generatorContext = getContext(relNode,
                    deltaInformations,
                    calciteContext,
                    enrichQueryRequest);
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
                } catch (RelConversionException e) {
                    promise.fail(new DtmException("Error in converting rel node", e));
                }
            }
            SqlNode sqlNodeResult = new NullNotCastableRelToSqlConverter(sqlDialect)
                    .visitChild(0, resultRelNode).asStatement();
            String queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql())
                    .replace("\n", " ");
            log.debug("sql = " + queryResult);
            promise.complete(queryResult);
        });
    }


    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext,
                                             EnrichQueryRequest enrichQueryRequest) {
        return new QueryGeneratorContext(
                deltaInformations.iterator(),
                enrichQueryRequest,
                calciteContext.getRelBuilder(),
                true,
                relNode);
    }
}
