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
package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.utils;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto.DeltaConditionContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import lombok.val;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.*;

public class SqlEnrichmentConditionUtil {
    private static final int ONE_LITERAL = 1;
    private static final int LIMIT_1 = 1;

    private SqlEnrichmentConditionUtil() {
    }

    public static List<RexNode> createDeltaCondition(List<DeltaInformation> deltaInformations,
                                                     RelBuilder relBuilder) {
        List<RexNode> conditions = deltaInformations.stream()
            .flatMap(deltaInfo -> {
                val conditionContext = DeltaConditionContext.builder()
                    .tableCount(deltaInformations.size())
                    .deltaInfo(deltaInfo)
                    .builder(relBuilder)
                    .finalize(false)
                    .build();

                switch (deltaInfo.getType()) {
                    case STARTED_IN:
                        return createRelNodeDeltaStartedIn(conditionContext).stream();
                    case FINISHED_IN:
                        return createRelNodeDeltaFinishedIn(conditionContext).stream();
                    case DATETIME:
                    case NUM:
                        return createRelNodeDeltaNum(conditionContext).stream();
                    default:
                        throw new DataSourceException(String.format("Incorrect delta type %s, expected values: %s!",
                            deltaInfo.getType(),
                            Arrays.toString(DeltaType.values())));
                }
            }).collect(Collectors.toList());
        deltaInformations.clear();
        return conditions;
    }

    private static List<RexNode> createRelNodeDeltaStartedIn(DeltaConditionContext ctx) {
        return Arrays.asList(
            ctx.getBuilder().call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                ctx.getBuilder().field(ctx.getDeltaInfo().getTableAlias(), SYS_FROM_FIELD),
                ctx.getBuilder().literal(ctx.getDeltaInfo().getSelectOnInterval().getSelectOnFrom())),
            ctx.getBuilder().call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                ctx.getBuilder().field(ctx.getDeltaInfo().getTableAlias(), SYS_FROM_FIELD),
                ctx.getBuilder().literal(ctx.getDeltaInfo().getSelectOnInterval().getSelectOnTo()))
        );
    }

    private static List<RexNode> createRelNodeDeltaFinishedIn(DeltaConditionContext ctx) {
        return Arrays.asList(
            ctx.getBuilder().call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                ctx.getBuilder().field(ctx.getDeltaInfo().getTableAlias(), SYS_TO_FIELD),
                ctx.getBuilder().literal(ctx.getDeltaInfo().getSelectOnInterval().getSelectOnFrom() - 1)),
            ctx.getBuilder().call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                ctx.getBuilder().field(ctx.getDeltaInfo().getTableAlias(), SYS_TO_FIELD),
                ctx.getBuilder().literal(ctx.getDeltaInfo().getSelectOnInterval().getSelectOnTo() - 1)),
            ctx.getBuilder().call(SqlStdOperatorTable.EQUALS,
                ctx.getBuilder().field(ctx.getDeltaInfo().getTableAlias(), SYS_OP_FIELD),
                ctx.getBuilder().literal(1))
        );
    }

    private static List<RexNode> createRelNodeDeltaNum(DeltaConditionContext ctx) {
        return Arrays.asList(
            ctx.getBuilder().call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                ctx.getBuilder().field(SYS_FROM_FIELD),
                ctx.getBuilder().literal(ctx.getDeltaInfo().getSelectOnNum())),
            ctx.getBuilder().call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                ctx.getBuilder().field(SYS_TO_FIELD),
                ctx.getBuilder().literal(ctx.getDeltaInfo().getSelectOnNum()))
        );
    }

    public static SqlBinaryOperator getSignOperatorCondition(boolean isTop) {
        return isTop ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;
    }

    public static RexNode createSignSubQuery(TableScan tableScan, boolean isTop) {
        val relBuilder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext())
            .create(tableScan.getCluster(), tableScan.getTable().getRelOptSchema());
        val node = relBuilder.scan(tableScan.getTable().getQualifiedName())
            .filter(relBuilder.call(SqlStdOperatorTable.LESS_THAN,
                relBuilder.field(SIGN_FIELD),
                relBuilder.literal(0)))
            .project(relBuilder.alias(relBuilder.literal(ONE_LITERAL), "r"))
            .limit(0, LIMIT_1)
            .build();
        return relBuilder.call(isTop ?
            SqlStdOperatorTable.IS_NOT_NULL : SqlStdOperatorTable.IS_NULL, RexSubQuery.scalar(node));
    }
}
