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
package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto.AdqmExtendContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.SYSTEM_FIELDS;
import static io.arenadata.dtm.query.execution.plugin.adqm.enrichment.utils.SqlEnrichmentConditionUtil.*;


@Slf4j
@Service("adqmDmlQueryExtendService")
public class AdqmDmlQueryExtendService implements QueryExtendService {
    private static final List<String> SYSTEM_FIELDS_PATTERNS = SYSTEM_FIELDS.stream()
            .map(sf -> sf + "(\\d+|)")
            .collect(Collectors.toList());
    private static final int SCHEMA_INDEX = 0;
    private static final int TABLE_NAME_INDEX = 1;
    private static final int BY_ONE_TABLE = 0;
    private static final int ONE_TABLE = 1;
    private final AdqmHelperTableNamesFactory helperTableNamesFactory;

    @Autowired
    public AdqmDmlQueryExtendService(AdqmHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    @Override
    public RelNode extendQuery(QueryGeneratorContext ctx) {
        val extendContext = new AdqmExtendContext();
        RelBuilder relBuilder = ctx.getRelBuilder();
        relBuilder.clear();
        val relNode = insertUnion(ctx, extendContext, relBuilder);
        relBuilder.clear();
        return relNode;
    }

    private RelNode insertUnion(QueryGeneratorContext ctx, AdqmExtendContext extendContext, RelBuilder relBuilder) {
        var relNode = iterateTree(ctx, extendContext, ctx.getRelNode().rel, ctx.getEnrichQueryRequest().isLocal());

        Aggregate aggregate = null;
        if (relNode instanceof Aggregate) {
            aggregate = (Aggregate) relNode;
            relNode = aggregate.getInput();
        }

        val topSignConditions = extendContext.getTableScans().stream()
                .map(tableScan -> createSignSubQuery(tableScan, true))
                .collect(Collectors.toList());

        val topNode = relBuilder
                .push(relNode)
                .filter(topSignConditions.size() == ONE_TABLE ?
                        topSignConditions.get(BY_ONE_TABLE) :
                        relBuilder.call(getSignOperatorCondition(true), topSignConditions))
                .build();

        val bottomSignConditions = extendContext.getTableScans().stream()
                .map(tableScan -> createSignSubQuery(tableScan, false))
                .collect(Collectors.toList());

        val bottomNode = relBuilder
                .push(relNode)
                .filter(bottomSignConditions.size() == ONE_TABLE ?
                        bottomSignConditions.get(BY_ONE_TABLE) :
                        relBuilder.call(getSignOperatorCondition(false), bottomSignConditions))
                .build();

        RelNode union = relBuilder
                .push(topNode)
                .push(bottomNode)
                .union(true)
                .build();

        if (aggregate != null) {
            relBuilder.push(aggregate.copy(aggregate.getTraitSet(), union, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList()));
            return relBuilder.build();
        }

        List<RexNode> withoutSystemFields = new ArrayList<>();
        for (int i = 0; i < union.getRowType().getFieldList().size(); i++) {
            RelDataTypeField fieldName = union.getRowType().getFieldList().get(i);
            val isSystemField = SYSTEM_FIELDS_PATTERNS.stream().anyMatch(fieldName.getName()::matches);
            if (isSystemField) {
                continue;
            }
            withoutSystemFields.add(union.getCluster().getRexBuilder().makeInputRef(union, i));
        }

        if (withoutSystemFields.size() == union.getRowType().getFieldList().size()) {
            return union;
        }

        return relBuilder.push(union)
                .project(withoutSystemFields)
                .build();
    }

    private RelNode iterateTree(QueryGeneratorContext context, AdqmExtendContext extendContext, RelNode node, boolean isLocal) {
        val deltaIterator = context.getDeltaIterator();
        val relBuilder = context.getRelBuilder();
        val newInput = new ArrayList<RelNode>();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                if (!context.getDeltaIterator().hasNext()) {
                    throw new DataSourceException("No parameters defined to enrich the request");
                }
                relBuilder.push(insertModifiedTableScan(context, extendContext, node, deltaIterator.next(), isLocal));
            } else {
                relBuilder.push(node);
            }
            return relBuilder.build();
        }

        if (node instanceof Filter) {
            val filter = (Filter) node;
            RexNode condition = iterateRexNode(context, extendContext, filter.getCondition());
            processInputs(context, extendContext, node, newInput, isLocal);

            if (newInput.get(0) instanceof Filter) {
                Filter previousFilter = (Filter) newInput.get(0);
                RexNode previousCondition = previousFilter.getCondition();
                relBuilder.push(previousFilter.copy(previousFilter.getTraitSet(), previousFilter.getInput(),
                        relBuilder.call(SqlStdOperatorTable.AND, RexUtil.flattenAnd(Arrays.asList(condition, previousCondition)))));
            } else {
                relBuilder.push(filter.copy(node.getTraitSet(), newInput.get(0), condition));
            }

            return relBuilder.build();
        }

        processInputs(context, extendContext, node, newInput, isLocal);

        if (node instanceof Project) {
            val project = (Project) node;
            val projects = project.getProjects();
            relBuilder.pushAll(newInput);
            addDeltaFiltersIfPresent(extendContext, relBuilder);

            return relBuilder
                    .project(projects)
                    .build();
        }

        if (extendContext.getDeltasToAdd().size() >= 2) {
            if (node instanceof Join) {
                val relNode = newInput.remove(1);
                val deltaInformation = extendContext.getDeltasToAdd().remove(1);
                val changedRightNode = relBuilder.push(relNode).filter(createDeltaConditions(relBuilder, deltaInformation)).build();
                newInput.add(changedRightNode);
            }
        }

        relBuilder.push(node.copy(node.getTraitSet(), newInput));
        addDeltaFiltersIfPresent(extendContext, relBuilder);
        return relBuilder.build();
    }

    private void addDeltaFiltersIfPresent(AdqmExtendContext extendContext, RelBuilder relBuilder) {

        if (!extendContext.getDeltasToAdd().isEmpty()) {
            val deltaConditions = new ArrayList<RexNode>();
            for (DeltaInformation deltaInformation : extendContext.getDeltasToAdd()) {
                deltaConditions.addAll(createDeltaConditions(relBuilder, deltaInformation));
            }

            if (relBuilder.peek() instanceof Filter) {
                val filter = (Filter) relBuilder.build();
                val condition = filter.getCondition();
                deltaConditions.add(condition);
                relBuilder.push(filter.getInput());
            }

            relBuilder.filter(relBuilder.call(SqlStdOperatorTable.AND, RexUtil.flattenAnd(deltaConditions)));
            extendContext.getDeltasToAdd().clear();
        }
    }

    private void processInputs(QueryGeneratorContext context, AdqmExtendContext extendContext, RelNode node, List<RelNode> newInput, boolean isLocal) {
        for (int i = 0; i < node.getInputs().size(); i++) {
            val input = node.getInputs().get(i);
            val isInputLocal = isLocal || isShard(node, i);
            newInput.add(iterateTree(context, extendContext, input, isInputLocal));
        }
    }

    private RexNode iterateRexNode(QueryGeneratorContext context, AdqmExtendContext extendContext, RexNode condition) {
        if (condition instanceof RexSubQuery) {
            val rexSubQuery = (RexSubQuery) condition;
            val relNode = iterateTree(context, extendContext, rexSubQuery.rel, true);
            return rexSubQuery.clone(relNode);
        }

        if (condition instanceof RexCall) {
            val rexCall = (RexCall) condition;

            val newOperands = new ArrayList<RexNode>();
            for (RexNode operand : rexCall.getOperands()) {
                newOperands.add(iterateRexNode(context, extendContext, operand));
            }

            return rexCall.clone(rexCall.type, newOperands);
        }

        return condition;
    }

    RelNode insertModifiedTableScan(QueryGeneratorContext ctx, AdqmExtendContext extendContext, RelNode tableScan, DeltaInformation deltaInfo, boolean isLocal) {
        val relBuilder = RelBuilder
                .proto(tableScan.getCluster().getPlanner().getContext())
                .create(tableScan.getCluster(), tableScan.getTable().getRelOptSchema());
        val qualifiedName = tableScan.getTable().getQualifiedName();
        val subRelNode = renameTableScan(ctx.getEnrichQueryRequest().getEnvName(), extendContext, deltaInfo, relBuilder, qualifiedName, isLocal);
        return relBuilder.push(subRelNode).build();
    }

    private RelNode renameTableScan(String env, AdqmExtendContext extendContext, DeltaInformation deltaInfo, RelBuilder relBuilder,
                                    List<String> qualifiedName, boolean isLocal) {
        val tableNames = helperTableNamesFactory.create(env,
                qualifiedName.get(SCHEMA_INDEX),
                qualifiedName.get(TABLE_NAME_INDEX));
        val tableName = isLocal ? tableNames.toQualifiedActualShard() : tableNames.toQualifiedActual();
        val scan = (TableScan) relBuilder
                .scan(tableName).build();
        extendContext.getTableScans().add(scan);
        extendContext.getDeltasToAdd().add(deltaInfo);
        return scan;
    }

    private boolean isShard(RelNode parentNode, int inputIndex) {
        return parentNode instanceof Join && inputIndex > 0;
    }
}
