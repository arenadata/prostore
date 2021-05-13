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
package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto.BuilderContext;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto.RelNodeContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.enrichment.utils.SqlEnrichmentConditionUtil.*;
import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.SYSTEM_FIELDS;


@Slf4j
@Service("adqmDmlQueryExtendService")
public class AdqmDmlQueryExtendServiceImpl implements QueryExtendService {
    private static final List<String> SYSTEM_FIELDS_PATTERNS = SYSTEM_FIELDS.stream()
        .map(sf -> sf + "(\\d+|)")
        .collect(Collectors.toList());
    private static final int SCHEMA_INDEX = 0;
    private static final int TABLE_NAME_INDEX = 1;
    private static final int BY_ONE_TABLE = 0;
    private static final int ONE_TABLE = 1;
    private final AdqmHelperTableNamesFactory helperTableNamesFactory;

    @Autowired
    public AdqmDmlQueryExtendServiceImpl(AdqmHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    @Override
    public RelNode extendQuery(QueryGeneratorContext ctx) {
        val rawRelationNode = ctx.getRelNode().rel;
        val physicalTableNames = iterateReplacingTableName(ctx, rawRelationNode, ctx.getEnrichQueryRequest().isLocal());
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
        BuilderContext builderCtx = null;
        val relBuilderMap = new HashMap<RelNode, BuilderContext>();
        for (Integer depth : depthSort) {
            val relNodeContexts = groupByDepth.get(depth).stream()
                .sorted(Comparator.comparing(RelNodeContext::getI))
                .collect(Collectors.toList());
            for (RelNodeContext nodeContext : relNodeContexts) {
                lastParent = nodeContext.getParent();
                if (nodeContext.getChild() instanceof TableScan) {
                    builderCtx = getOrCreateBuilderCtxByParent(ctx, lastParent, relBuilderMap);
                    processTableScan(builderCtx, nodeContext);
                } else {
                    builderCtx = getOrCreateBuilderCtxByChild(ctx, relBuilderMap, nodeContext);
                    builderCtx.setLastChildNode(nodeContext.getChild());
                    if (nodeContext.getChild() instanceof Join) {
                        processJoin(builderCtx, (Join) nodeContext.getChild());
                    } else if (nodeContext.getChild() instanceof Filter) {
                        processFilter(builderCtx, nodeContext);
                    } else if (nodeContext.getChild() instanceof Project) {
                        processProject(builderCtx, (Project) nodeContext.getChild());
                    } else if (nodeContext.getChild() instanceof Sort) {
                        processSort(builderCtx, (Sort) nodeContext.getChild());
                    } else if (nodeContext.getChild() instanceof Aggregate) {
                        processAggregate(builderCtx, (Aggregate) nodeContext.getChild());
                    } else {
                        processOther(builderCtx, nodeContext);
                    }
                }
                refreshBuilderContextCache(builderCtx, relBuilderMap, nodeContext);
            }
        }
        if (builderCtx != null) {
            val resultEnrichmentNode = processLastParentNode(lastParent, builderCtx, ctx);
            return removeUnnecessaryCast(resultEnrichmentNode);
        } else {
            throw new DataSourceException("Can't enrich query for ADQM: Query is not valid");
        }
    }

    private RelNode iterateReplacingTableName(QueryGeneratorContext context, RelNode node, boolean isLocal) {
        List<RelNode> newInput = new ArrayList<>();
        val relBuilder = context.getRelBuilder();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                relBuilder.push(insertRenameTableScan(context, node, isLocal));
            }
            return relBuilder.build();
        }
        for (int i = 0; i < node.getInputs().size(); i++) {
            RelNode input = node.getInput(i);
            newInput.add(iterateReplacingTableName(context, input, isLocal || isShard(node, input, i)));
        }
        relBuilder.push(node.copy(node.getTraitSet(), newInput));
        return relBuilder.build();
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
                    .withSchemaPath(ctx.getEnrichQueryRequest()
                        .getDeltaInformations().stream()
                        .map(DeltaInformation::getSchemaName)
                        .distinct()
                        .collect(Collectors.toList())));
        val qualifiedName = tableScan.getTable().getQualifiedName();
        val queryRequest = ctx.getEnrichQueryRequest();
        val tableNames = helperTableNamesFactory.create(queryRequest.getEnvName(),
            qualifiedName.get(SCHEMA_INDEX),
            qualifiedName.get(TABLE_NAME_INDEX));
        val tableName = isShard ? tableNames.toQualifiedActualShard() : tableNames.toQualifiedActual();
        return relBuilder.scan(tableName).build();
    }

    private void refreshBuilderContextCache(BuilderContext builderCtx, HashMap<RelNode, BuilderContext> relBuilderMap, RelNodeContext nodeContext) {
        if (!relBuilderMap.containsKey(nodeContext.getParent())) {
            relBuilderMap.put(nodeContext.getParent(), builderCtx);
        } else if (builderCtx.getBuilder().peek() != null) {
            relBuilderMap.get(nodeContext.getParent()).getBuilders().add(builderCtx.getBuilder());
        }
    }

    private void processOther(BuilderContext builderCtx, RelNodeContext nodeContext) {
        val relBuilder = builderCtx.getBuilder();
        relBuilder.push(
            nodeContext.getChild().copy(
                nodeContext.getChild().getTraitSet(),
                Collections.singletonList(relBuilder.build())
            )
        );
    }

    private void processSort(BuilderContext builderCtx, Sort sort) {
        val relBuilder = builderCtx.getBuilder();
        int fetch = sort.fetch == null ? -1 : Integer.parseInt(sort.fetch.toString());
        int offset = sort.offset == null ? -1 : Integer.parseInt(sort.offset.toString());
        relBuilder.sortLimit(offset, fetch, relBuilder.fields(sort.getCollation()));
    }

    private void processAggregate(BuilderContext builderCtx, Aggregate aggregate) {
        val relBuilder = builderCtx.getBuilder();
        val relNode = relBuilder.build();
        relBuilder.push(aggregate.copy(relNode.getTraitSet(),
            relNode,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList()));
    }

    private RelNode filterSystemFields(QueryGeneratorContext ctx, RelNode physicalTableNames) {
        val logicalFields = getLogicalFields(physicalTableNames
            .getRowType()
            .getFieldNames());
        val relBuilder = ctx.getRelBuilder();
        return relBuilder
            .push(physicalTableNames)
            .project(relBuilder.fields(logicalFields))
            .build();
    }

    private List<String> getLogicalFields(List<String> fieldNames) {
        return fieldNames.stream()
            .filter(fieldName -> SYSTEM_FIELDS_PATTERNS.stream().noneMatch(fieldName::matches))
            .collect(Collectors.toList());
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
                val nodeContext = RelNodeContext.builder()
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

    private void processTableScan(BuilderContext builderCtx, RelNodeContext nodeContext) {
        val relBuilder = builderCtx.getBuilder();
        relBuilder.scan(nodeContext.getChild().getTable().getQualifiedName())
            .as(nodeContext.getDeltaInformation().getTableAlias());
        val tableScan = (TableScan) nodeContext.getChild();
        builderCtx.getTableScans().add(tableScan);
        if (isUsingJoinBlock(nodeContext)) {
            List<RexNode> deltaConditions = createDeltaCondition(Lists.newArrayList(nodeContext.getDeltaInformation()),
                relBuilder);
            relBuilder.filter(deltaConditions);
        } else {
            builderCtx.getDeltaInformations().add(nodeContext.getDeltaInformation());
            builderCtx.setLastChildNode(nodeContext.getChild());
        }
    }

    private boolean isUsingJoinBlock(RelNodeContext nodeContext) {
        return nodeContext.getParent() instanceof Join
            && ((Join) nodeContext.getParent()).getRight().equals(nodeContext.getChild());
    }

    private void processJoin(BuilderContext builderCtx, Join join) {
        val relBuilder = builderCtx.getBuilder();
        relBuilder.join(join.getJoinType(), join.getCondition());
    }

    private void processProject(BuilderContext builderCtx, Project project) {
        val relBuilder = builderCtx.getBuilder();
        if (relBuilder.peek() instanceof Project) {
            return;
        }
        if (!builderCtx.getDeltaInformations().isEmpty()) {
            val deltaConditions = createDeltaCondition(builderCtx.getDeltaInformations(), relBuilder);
            val allDeltaConditions = deltaConditions.size() == 1 ?
                deltaConditions.get(0) : relBuilder.call(SqlStdOperatorTable.AND, deltaConditions);
            relBuilder.filter(allDeltaConditions);
        }
        if (!builderCtx.getTableScans().isEmpty()) {
            addSignConditions(builderCtx, relBuilder.build());
            builderCtx.getTableScans().clear();
        }
        relBuilder
            .project(project.getChildExps());
    }

    private void processFilter(BuilderContext ctx, RelNodeContext nodeContext) {
        val filter = (Filter) nodeContext.getChild();
        val condition = filter.getCondition();
        val builder = ctx.getBuilder();
        val deltaConditions = createDeltaCondition(ctx.getDeltaInformations(), builder);
        deltaConditions.add(condition);
        builder.filter(builder.call(SqlStdOperatorTable.AND, deltaConditions));
    }

    private void addSignConditions(BuilderContext ctx, RelNode relNode) {
        val relBuilder = ctx.getBuilder();
        relBuilder.push(relNode);
        val project = relBuilder
            .project(relBuilder.fields(), relNode.getRowType().getFieldNames(), true)
            .build();
        val topSignConditions = ctx.getTableScans().stream()
            .map(tableScan -> createSignSubQuery(tableScan, true))
            .collect(Collectors.toList());

        val topNode = relBuilder
            .push(project)
            .filter(topSignConditions.size() == ONE_TABLE ?
                topSignConditions.get(BY_ONE_TABLE) :
                relBuilder.call(getSignOperatorCondition(true), topSignConditions))
            .build();

        val bottomSignConditions = ctx.getTableScans().stream()
            .map(tableScan -> createSignSubQuery(tableScan, false))
            .collect(Collectors.toList());

        val bottomNode = relBuilder
            .push(project)
            .filter(bottomSignConditions.size() == ONE_TABLE ?
                bottomSignConditions.get(BY_ONE_TABLE) :
                relBuilder.call(getSignOperatorCondition(false), bottomSignConditions))
            .build();
        relBuilder.push(topNode)
            .push(bottomNode)
            .union(true);
    }

    private BuilderContext getOrCreateBuilderCtxByParent(QueryGeneratorContext ctx,
                                                         RelNode lastParent,
                                                         HashMap<RelNode, BuilderContext> relBuilderMap) {
        if (relBuilderMap.containsKey(lastParent)) {
            return relBuilderMap.get(lastParent);
        } else {
            return getBuilderContext(ctx);
        }
    }

    private BuilderContext getOrCreateBuilderCtxByChild(QueryGeneratorContext context,
                                                        HashMap<RelNode, BuilderContext> relBuilderMap,
                                                        RelNodeContext nodeContext) {
        if (relBuilderMap.containsKey(nodeContext.getChild())) {
            return relBuilderMap.get(nodeContext.getChild());
        } else {
            return getBuilderContext(context);
        }
    }

    private BuilderContext getBuilderContext(QueryGeneratorContext context) {
        return BuilderContext.builder()
            .builders(Lists.newArrayList(getRelBuilder(context)))
            .deltaInformations(new ArrayList<>())
            .tableScans(new ArrayList<>())
            .build();
    }

    private RelNode processLastParentNode(RelNode lastParent,
                                          BuilderContext buildCtx,
                                          QueryGeneratorContext ctx) {
        RelBuilder relBuilder = buildCtx.getBuilder();
        if (lastParent instanceof Join) {
            processJoin(buildCtx, (Join) lastParent);
        } else if (lastParent instanceof Sort) {
            processSort(buildCtx, (Sort) lastParent);
        } else if (lastParent instanceof Aggregate) {
            processAggregate(buildCtx, (Aggregate) lastParent);
        } else if (lastParent instanceof Project) {
            processProject(buildCtx, (Project) lastParent);
        }
        Optional<RelNode> relNodeOpt = tryTrimUnusedSortColumn(relBuilder, ctx.getRelNode());
        if (relNodeOpt.isPresent()) {
            return relNodeOpt.get();
        } else {
            filterSystemFields(relBuilder);
            return relBuilder.build();
        }
    }


    private Optional<RelNode> tryTrimUnusedSortColumn(RelBuilder relBuilder, RelRoot relRoot) {
        val sourceRelNode = relBuilder.peek();
        if (sourceRelNode instanceof LogicalSort) {
            List<String> logicalFields = getLogicalFields(relRoot.validatedRowType.getFieldNames());
            if (sourceRelNode.getRowType().getFieldCount() != logicalFields.size()) {
                relBuilder.push(sourceRelNode);
                ImmutableList<RexNode> fields = relBuilder.fields(logicalFields);
                return Optional.of(relBuilder.project(fields).build());
            }
        }
        return Optional.empty();
    }

    private void filterSystemFields(RelBuilder relBuilder) {
        val fieldNames = relBuilder.peek().getRowType()
            .getFieldNames();
        val logicalFields = getLogicalFields(fieldNames);
        if (fieldNames.size() > logicalFields.size()) {
            relBuilder
                .project(relBuilder.fields(logicalFields));
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
            .getEnrichQueryRequest()
            .getDeltaInformations().stream()
            .map(DeltaInformation::getSchemaName)
            .distinct()
            .collect(Collectors.toList());

        return RelBuilder.proto(ctx.getRelNode().rel.getCluster().getPlanner().getContext())
            .create(ctx.getRelNode().rel.getCluster(),
                ((CalciteCatalogReader) ctx.getRelBuilder().getRelOptSchema())
                    .withSchemaPath(schemaPaths));
    }
}
