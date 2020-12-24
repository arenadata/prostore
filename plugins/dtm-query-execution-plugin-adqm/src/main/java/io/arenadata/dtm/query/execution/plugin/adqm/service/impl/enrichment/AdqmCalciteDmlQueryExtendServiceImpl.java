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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryExtendService;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.*;


@Slf4j
@Service("adqmCalciteDmlQueryExtendService")
public class AdqmCalciteDmlQueryExtendServiceImpl implements QueryExtendService {
    private final static List<String> SYSTEM_FIELDS_PATTERNS = SYSTEM_FIELDS.stream()
        .map(sf -> sf + "(\\d+|)")
        .collect(Collectors.toList());
    private static final int SCHEMA_INDEX = 0;
    private static final int TABLE_NAME_INDEX = 1;
    private static final int ONE_LITERAL = 1;
    private static final int BY_ONE_TABLE = 0;
    private static final int ONE_TABLE = 1;
    private static final int LIMIT_1 = 1;
    private final AdqmHelperTableNamesFactory helperTableNamesFactory;

    public AdqmCalciteDmlQueryExtendServiceImpl(AdqmHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    @Override
    public RelNode extendQuery(QueryGeneratorContext ctx) {
        val rawRelationNode = ctx.getRelNode().rel;
        val physicalTableNames = iterateReplacingTableName(ctx, rawRelationNode, ctx.isLocal());
        val withoutSystemFields = filterSystemFields(ctx, physicalTableNames);
        val allRelNodeCtxs = getRelNodeContexts(ctx, withoutSystemFields);
        val groupByDepth = allRelNodeCtxs.stream()
            .collect(Collectors.groupingBy(RelNodeContext::getDepth, Collectors.toList()));
        val depthSort = groupByDepth.keySet().stream()
            .sorted(Comparator.reverseOrder())
            .collect(Collectors.toList());
        if (depthSort.isEmpty()) {
            return withoutSystemFields;
        }
        RelNode lastParent = null;
        BuilderCondext builderCtx = null;
        val relBuilderMap = new HashMap<RelNode, BuilderCondext>();
        for (Integer depth : depthSort) {
            val relNodeContexts = groupByDepth.get(depth).stream()
                .sorted(Comparator.comparing(RelNodeContext::getI))
                .collect(Collectors.toList());
            for (RelNodeContext nodeContext : relNodeContexts) {
                lastParent = nodeContext.parent;
                if (nodeContext.child instanceof TableScan) {
                    builderCtx = getOrCreateBuilderCtxByParent(ctx, lastParent, relBuilderMap);
                    proccessTableScan(builderCtx, nodeContext);
                } else {
                    builderCtx = getOrCreateBuilderCtxByChild(ctx, relBuilderMap, nodeContext);
                    builderCtx.setLastChildNode(nodeContext.getChild());
                    if (nodeContext.child instanceof Join) {
                        processJoin(builderCtx, nodeContext);
                    } else if (nodeContext.child instanceof Filter) {
                            processFilter(builderCtx, nodeContext);
//                        if (depth == 0) {
//                        } else {
//                            val byLastUnion = new ArrayList<>(builderCtx.getDeltaInformations());
//                            processFilter(builderCtx, nodeContext);
//                            builderCtx.getDeltaInformations().addAll(byLastUnion);
//                        }
                    } else if (nodeContext.child instanceof Project) {
                        processProject(builderCtx, nodeContext);
                    } else if (nodeContext.child instanceof Aggregate) {
                        builderCtx.getBuilder().push(
                            nodeContext.child.copy(
                                nodeContext.child.getTraitSet(),
                                Collections.singletonList(builderCtx.getBuilder().build())
                            )
                        );
                        if (nodeContext.getParent() instanceof Project) {
                            Project parent = (Project) nodeContext.getParent();
                            builderCtx.getBuilder().project(parent.getChildExps());
                        }
                    } else {
                        builderCtx.getBuilder().push(
                            nodeContext.child.copy(
                                nodeContext.child.getTraitSet(),
                                Collections.singletonList(builderCtx.getBuilder().build())
                            )
                        );
                    }
                }
                if (!relBuilderMap.containsKey(nodeContext.getParent())) {
                    relBuilderMap.put(nodeContext.getParent(), builderCtx);
                }
            }
        }

        val resultEnrichmentNode = getResultEnrichmentNode(lastParent, builderCtx);
        return removeUnnecessaryCast(resultEnrichmentNode);
    }

    private RelNode filterSystemFields(QueryGeneratorContext ctx, RelNode physicalTableNames) {
        val logicalFields = physicalTableNames
            .getRowType()
            .getFieldNames().stream()
            .filter(fieldName -> SYSTEM_FIELDS_PATTERNS.stream().noneMatch(fieldName::matches))
            .collect(Collectors.toList());
        return ctx.getRelBuilder()
            .push(physicalTableNames)
            .project(ctx.getRelBuilder().fields(logicalFields))
            .build();
    }

    private List<RelNodeContext> getRelNodeContexts(QueryGeneratorContext ctx, RelNode replacingTablesNode) {
        List<RelNodeContext> contexts = new ArrayList<>();
        Map<RelNode, Integer> contextMap = new HashMap<>();
        replacingTablesNode.accept(new RelHomogeneousShuttle() {
            int id = 0;

            @Override
            protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                int depth = 0;
                if (contextMap.containsKey(parent)) {
                    depth = contextMap.get(parent) + 1;
                }
                RelNodeContext nodeContext = RelNodeContext.builder()
                    .deltaInformation(child instanceof TableScan ? ctx.getDeltaIterator().next() : null)
                    .parent(parent)
                    .child(child)
                    .depth(depth)
                    .i(id++)
                    .build();
                contexts.add(nodeContext);
                contextMap.put(child, depth);
                return super.visitChild(parent, i, child);
            }
        });
        return contexts;
    }

    private void proccessTableScan(BuilderCondext builderCtx, RelNodeContext nodeContext) {
        builderCtx.setLastChildNode(nodeContext.getChild());
        TableScan tableScan = (TableScan) nodeContext.child;
        builderCtx.getTableScans().add(tableScan);
        builderCtx.getDeltaInformations().add(nodeContext.deltaInformation);
        builderCtx.getBuilder().scan(nodeContext.getChild().getTable().getQualifiedName())
            .as(nodeContext.deltaInformation.getTableAlias());
    }

    private void processJoin(BuilderCondext builderCtx, RelNodeContext nodeContext) {
        val join = (Join) nodeContext.child;
        builderCtx.getBuilder().join(join.getJoinType(), join.getCondition());
    }

    private void processProject(BuilderCondext builderCtx, RelNodeContext nodeContext) {
        val project = (Project) nodeContext.child;
        if (builderCtx.getDeltaInformations().size() > 0) {
            val deltaConditions = getDeltaConditions(builderCtx.getDeltaInformations(), builderCtx.getBuilder());
            val allDeltaConditions = deltaConditions.size() == 1 ?
                deltaConditions.get(0) : builderCtx.getBuilder().call(SqlStdOperatorTable.AND, deltaConditions);
            builderCtx.getBuilder().filter(allDeltaConditions);
        }
        builderCtx.getBuilder()
            .project(project.getChildExps());
        if (builderCtx.getTableScans().size() > 0) {
            addSignConditions(builderCtx, builderCtx.getBuilder().build());
            builderCtx.getTableScans().clear();
        }
    }

    private void processFilter(BuilderCondext ctx, RelNodeContext nodeContext) {
        val filter = (Filter) nodeContext.child;
        val condition = filter.getCondition();
        val deltaConditions = getDeltaConditions(ctx.getDeltaInformations(), ctx.getBuilder());
        deltaConditions.add(condition);
        ctx.getBuilder().filter(ctx.getBuilder().call(SqlStdOperatorTable.AND, deltaConditions));
    }

    private void addSignConditions(BuilderCondext ctx, RelNode relNode) {
        List<String> fieldNames = relNode.getRowType().getFieldNames().stream()
            .filter(f -> SYSTEM_FIELDS_PATTERNS.stream().noneMatch(f::matches))
            .collect(Collectors.toList());

        RelNode project = ctx.getBuilder()
            .push(relNode)
            .project(ctx.getBuilder().fields(fieldNames))
            .build();

        val topSignConditions = ctx.getTableScans().stream()
            .map(tableScan -> createSignSubQuery(tableScan, true))
            .collect(Collectors.toList());

        val topNode = ctx.getBuilder()
            .push(project)
            .filter(topSignConditions.size() == ONE_TABLE ?
                topSignConditions.get(BY_ONE_TABLE) :
                ctx.getBuilder().call(getSignOperatorCondition(true), topSignConditions))
            .build();

        val bottomSignConditions = ctx.getTableScans().stream()
            .map(tableScan -> createSignSubQuery(tableScan, false))
            .collect(Collectors.toList());

        val bottomNode = ctx.getBuilder()
            .push(project)
            .filter(bottomSignConditions.size() == ONE_TABLE ?
                bottomSignConditions.get(BY_ONE_TABLE) :
                ctx.getBuilder().call(getSignOperatorCondition(false), bottomSignConditions))
            .build();

        ctx.getBuilder().push(topNode)
            .push(bottomNode)
            .union(true);

    }

    private RelNode iterateReplacingTableName(QueryGeneratorContext context, RelNode node, boolean isLocal) {
        List<RelNode> newInput = new ArrayList<>();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                context.getRelBuilder().push(insertRenameTableScan(context, node, isLocal));
            }
            return context.getRelBuilder().build();
        }
        for (int i = 0; i < node.getInputs().size(); i++) {
            RelNode input = node.getInput(i);
            newInput.add(iterateReplacingTableName(context, input, isLocal || isShard(node, input, i)));
        }
        context.getRelBuilder().push(node.copy(node.getTraitSet(), newInput));
        return context.getRelBuilder().build();
    }

    private boolean isShard(RelNode parentNode, RelNode node, int i) {
        return node instanceof TableScan && parentNode instanceof Join && i > 0;
    }

    RelNode insertRenameTableScan(QueryGeneratorContext ctx,
                                  RelNode tableScan,
                                  boolean isShard) {
        val relBuilder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext())
            .create(tableScan.getCluster(),
                ((CalciteCatalogReader) ctx.getRelBuilder().getRelOptSchema())
                    .withSchemaPath(ctx.getQueryRequest()
                        .getDeltaInformations().stream()
                        .map(DeltaInformation::getSchemaName)
                        .distinct()
                        .collect(Collectors.toList())));
        val qualifiedName = tableScan.getTable().getQualifiedName();
        val queryRequest = ctx.getQueryRequest();
        val tableNames = helperTableNamesFactory.create(queryRequest.getEnvName(),
            qualifiedName.get(SCHEMA_INDEX),
            qualifiedName.get(TABLE_NAME_INDEX));
        val tableName = isShard ? tableNames.toQualifiedActualShard() : tableNames.toQualifiedActual();
        return relBuilder.scan(tableName).build();
    }


    private SqlBinaryOperator getSignOperatorCondition(boolean isTop) {
        return isTop ? SqlStdOperatorTable.AND : SqlStdOperatorTable.OR;
    }

    private RexNode createSignSubQuery(TableScan tableScan, boolean isTop) {
        val builder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext())
            .create(tableScan.getCluster(), tableScan.getTable().getRelOptSchema());
        val node = builder.scan(tableScan.getTable().getQualifiedName())
            .filter(builder.call(SqlStdOperatorTable.LESS_THAN,
                builder.field(SIGN_FIELD),
                builder.literal(0)))
            .project(builder.alias(builder.literal(ONE_LITERAL), "r"))
            .limit(0, LIMIT_1)
            .build();
        return builder.call(isTop ?
            SqlStdOperatorTable.IS_NOT_NULL : SqlStdOperatorTable.IS_NULL, RexSubQuery.scalar(node));
    }

    private BuilderCondext getOrCreateBuilderCtxByParent(QueryGeneratorContext ctx,
                                                         RelNode lastParent,
                                                         HashMap<RelNode, BuilderCondext> relBuilderMap) {
        if (relBuilderMap.containsKey(lastParent)) {
            return relBuilderMap.get(lastParent);
        } else {
            return BuilderCondext.builder()
                .builder(getRelBuilder(ctx))
                .deltaInformations(new ArrayList<>())
                .tableScans(new ArrayList<>())
                .build();
        }
    }

    private BuilderCondext getOrCreateBuilderCtxByChild(QueryGeneratorContext context,
                                                        HashMap<RelNode, BuilderCondext> relBuilderMap,
                                                        RelNodeContext nodeContext) {
        if (relBuilderMap.containsKey(nodeContext.child)) {
            return relBuilderMap.get(nodeContext.child);
        } else {
            return BuilderCondext.builder()
                .builder(getRelBuilder(context))
                .deltaInformations(new ArrayList<>())
                .tableScans(new ArrayList<>())
                .build();
        }
    }

    private RelNode getResultEnrichmentNode(RelNode lastParent, BuilderCondext buildCtx) {
        if (lastParent instanceof Join) {
            val join = (Join) buildCtx.getLastChildNode();
            buildCtx.getBuilder().join(join.getJoinType(), join.getCondition());
        }
        if (buildCtx.getDeltaInformations().size() > 0) {
            val deltaConditions = getDeltaConditions(buildCtx.getDeltaInformations(), buildCtx.getBuilder());
            val allDeltaConditions = deltaConditions.size() == 1 ?
                deltaConditions.get(0) : buildCtx.getBuilder().call(SqlStdOperatorTable.AND, deltaConditions);
            buildCtx.getBuilder().filter(allDeltaConditions);
            val queryNode = buildCtx.getBuilder()
                .build();
            addSignConditions(buildCtx, queryNode);
        } else if (buildCtx.getTableScans().size() > 0) {
            addSignConditions(buildCtx, buildCtx.getBuilder().project(lastParent.getChildExps()).build());
        }

        val lastNode = buildCtx.getBuilder().build();
        if (lastParent instanceof Project) {
            return lastNode;
        } else {
            return lastParent.copy(lastNode.getTraitSet(), Collections.singletonList(lastNode));
        }

    }

    private RelNode removeUnnecessaryCast(RelNode relNode) {
        return removeUnnecessaryCastInChildren(relNode)
            .accept(new RelShuttleImpl() {
                @Override
                protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                    stack.push(parent);
                    try {
                        var inChildren = removeUnnecessaryCastInChildren(child);
                        inChildren = inChildren.accept(this);
                        if (inChildren != child) {
                            final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                            newInputs.set(i, inChildren);
                            return parent.copy(parent.getTraitSet(), newInputs);
                        }
                        return parent;
                    } finally {
                        stack.pop();
                    }
                }
            });
    }

    private RelNode removeUnnecessaryCastInChildren(RelNode rel) {
        return rel.accept(new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (SqlKind.CAST.equals(call.getOperator().getKind())) {
                    val castType = call.getType().getSqlTypeName();
                    val operandNode = call.getOperands().get(0);
                    val operandType = operandNode.getType().getSqlTypeName();
                    if (operandType.equals(castType)) {
                        return operandNode;
                    }
                }
                return super.visitCall(call);
            }
        });
    }

    private RelBuilder getRelBuilder(QueryGeneratorContext ctx) {
        val schemaPaths = ctx
            .getQueryRequest()
            .getDeltaInformations().stream()
            .map(DeltaInformation::getSchemaName)
            .distinct()
            .collect(Collectors.toList());

        return RelBuilder.proto(ctx.getRelNode().rel.getCluster().getPlanner().getContext())
            .create(ctx.getRelNode().rel.getCluster(),
                ((CalciteCatalogReader) ctx.getRelBuilder().getRelOptSchema())
                    .withSchemaPath(schemaPaths));
    }

    private List<RexNode> getDeltaConditions(List<DeltaInformation> deltaInformations,
                                             RelBuilder builder) {
        List<RexNode> conditions = deltaInformations.stream()
            .flatMap(deltaInfo -> {
                val conditionContext = DeltaConditionContext.builder()
                    .tableCount(deltaInformations.size())
                    .deltaInfo(deltaInfo)
                    .builder(builder)
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
                        throw new RuntimeException(String.format("Incorrect delta type %s, expected values: %s!",
                            deltaInfo.getType(), Arrays.toString(DeltaType.values())));
                }
            }).collect(Collectors.toList());
        deltaInformations.clear();
        return conditions;
    }

    private List<RexNode> createRelNodeDeltaStartedIn(DeltaConditionContext ctx) {
        return Arrays.asList(
            ctx.builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                ctx.builder.field(ctx.deltaInfo.getTableAlias(), SYS_FROM_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getSelectOnInterval().getSelectOnFrom())),
            ctx.builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                ctx.builder.field(ctx.deltaInfo.getTableAlias(), SYS_FROM_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getSelectOnInterval().getSelectOnTo()))
        );
    }

    private List<RexNode> createRelNodeDeltaFinishedIn(DeltaConditionContext ctx) {
        return Arrays.asList(
            ctx.builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                ctx.builder.field(ctx.deltaInfo.getTableAlias(), SYS_TO_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getSelectOnInterval().getSelectOnFrom() - 1)),
            ctx.builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                ctx.builder.field(ctx.deltaInfo.getTableAlias(), SYS_TO_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getSelectOnInterval().getSelectOnTo() - 1)),
            ctx.builder.call(SqlStdOperatorTable.EQUALS,
                ctx.builder.field(ctx.deltaInfo.getTableAlias(), SYS_OP_FIELD),
                ctx.builder.literal(1))
        );
    }

    private List<RexNode> createRelNodeDeltaNum(DeltaConditionContext ctx) {
        return Arrays.asList(
            ctx.builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                ctx.builder.field(ctx.deltaInfo.getTableAlias(), SYS_FROM_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getSelectOnNum())),
            ctx.builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                ctx.builder.field(ctx.deltaInfo.getTableAlias(), SYS_TO_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getSelectOnNum()))
        );
    }

    @Data
    @Builder
    private static final class DeltaConditionContext {
        private DeltaInformation deltaInfo;
        private RelBuilder builder;
        private boolean finalize;
        private int tableCount;
    }

    @Data
    @Builder
    private static final class RelNodeContext {
        private DeltaInformation deltaInformation;
        private RelNode parent;
        private RelNode child;
        private int childCount;
        private int depth;
        private int i;
    }

    @Data
    @Builder
    private static final class BuilderCondext {
        private List<DeltaInformation> deltaInformations;
        private List<TableScan> tableScans;
        private RelNode lastChildNode;
        private RelBuilder builder;
    }

}
