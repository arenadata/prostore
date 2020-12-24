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
package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.dto.dml.DatamartViewPair;
import io.arenadata.dtm.query.execution.core.dto.dml.DatamartViewWrap;
import io.arenadata.dtm.query.execution.core.dto.dml.ReplaceContext;
import io.arenadata.dtm.query.execution.core.dto.dml.ViewReplaceAction;
import io.arenadata.dtm.query.execution.core.service.dml.DatamartViewWrapLoader;
import io.arenadata.dtm.query.execution.core.service.dml.LogicViewReplacer;
import io.arenadata.dtm.query.execution.core.service.dml.SqlSnapshotReplacer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Slf4j
@Component
public class LogicViewReplacerImpl implements LogicViewReplacer {
    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final DefinitionService<SqlNode> definitionService;
    private final SqlSnapshotReplacer snapshotReplacer;
    private final DatamartViewWrapLoader viewLoader;

    public LogicViewReplacerImpl(
            @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
            SqlSnapshotReplacer snapshotReplacer,
            DatamartViewWrapLoader viewLoader
    ) {
        this.definitionService = definitionService;
        this.snapshotReplacer = snapshotReplacer;
        this.viewLoader = viewLoader;
    }

    @SneakyThrows
    @Override
    public void replace(String sql,
                        String datamart,
                        Handler<AsyncResult<String>> resultHandler) {
        log.debug("before replacing:\n{}", sql);
        val ctx = getReplaceContext(sql, datamart, resultHandler);
        preparing(ctx);
        replace(ctx);
    }

    @NotNull
    private ReplaceContext getReplaceContext(String sql,
                                             String datamart,
                                             Handler<AsyncResult<String>> resultHandler) {
        val sqlNode = definitionService.processingQuery(sql);
        return new ReplaceContext(sqlNode, datamart, resultHandler);
    }

    private void preparing(ReplaceContext ctx) {
        val tempActions = ctx.getTempActions();
        processSqlNode(ctx.getRootSqlNode(), false, tempActions, ignore());
    }

    private void replace(ReplaceContext ctx) {
        val resultActions = ctx.getResultActions();
        val tempActions = ctx.getTempActions();
        val viewsPairByLoad = getViewsPairByLoad(ctx, tempActions);
        viewLoader.loadViews(viewsPairByLoad)
                .onSuccess(viewWraps -> {
                    refreshCtxTables(ctx, viewsPairByLoad, viewWraps);
                    refreshCtxViews(ctx, viewWraps);
                    val filteredActions = getViewActions(ctx.getViewMap(), tempActions);
                    tempActions.clear();
                    setSnapshots(filteredActions);
                    wrapAlias(filteredActions);
                    if (filteredActions.size() > 0) {
                        resultActions.addAll(filteredActions);
                        filteredActions.forEach(a -> processSqlNode(a.getTo(), false, tempActions, ignore()));
                    }
                    if (tempActions.size() > 0) {
                        replace(ctx);
                    } else {
                        for (int i = resultActions.size() - 1; i >= 0; i--) {
                            resultActions.get(i).run();
                        }
                        val sql = ctx.getRootSqlNode().toSqlString(SQL_DIALECT).getSql();
                        log.debug("after replacing:\n{}", sql);
                        ctx.getResultHandler().handle(Future.succeededFuture(sql));
                    }
                })
                .onFailure(fail -> ctx.getResultHandler().handle(Future.failedFuture(fail)));
    }

    private Set<DatamartViewPair> getViewsPairByLoad(ReplaceContext ctx, List<ViewReplaceAction> actions) {
        return actions.stream()
                .map(ViewReplaceAction::getViewPair)
                .peek(pair -> setDefaultDatamart(ctx, pair))
                .filter(pair -> !ctx.getTables().contains(pair))
                .filter(pair -> !ctx.getViewMap().containsKey(pair))
                .collect(Collectors.toSet());
    }

    private void setDefaultDatamart(ReplaceContext ctx, DatamartViewPair a) {
        if (a.getDatamart() == null) a.setDatamart(ctx.getDefaultDatamart());
    }

    private void refreshCtxViews(ReplaceContext ctx, List<DatamartViewWrap> viewWraps) {
        viewWraps.forEach(datamartViewWrap
                -> ctx.getViewMap().put(datamartViewWrap.getPair(), datamartViewWrap));
    }

    private void refreshCtxTables(ReplaceContext ctx,
                                  Set<DatamartViewPair> viewsPairByLoad,
                                  List<DatamartViewWrap> viewWraps) {
        viewsPairByLoad.stream()
                .filter(pair -> viewWraps.stream().noneMatch(w -> w.getPair().equals(pair)))
                .forEach(pair -> ctx.getTables().add(pair));
    }

    private void wrapAlias(List<ViewReplaceAction> filteredActions) {
        filteredActions.stream()
                .filter(ViewReplaceAction::isNeedWrap)
                .forEach(a -> a.setTo(createAlias(a)));
    }

    private SqlNode createAlias(ViewReplaceAction a) {
        val replacementNode = a.getTo();
        val parserPosition = replacementNode.getParserPosition();
        val sqlIdentifier = new SqlIdentifier(a.getViewPair().getViewName(), parserPosition);
        return new SqlBasicCall(
                SqlStdOperatorTable.AS,
                new SqlNode[]{replacementNode, sqlIdentifier},
                parserPosition
        );
    }

    private void setSnapshots(List<ViewReplaceAction> filteredActions) {
        filteredActions.stream()
                .filter(a -> a.getFrom() instanceof SqlSnapshot)
                .forEach(a -> snapshotReplacer.replace((SqlSnapshot) a.getFrom(), (SqlSelect) a.getTo()));
    }

    @NotNull
    private List<ViewReplaceAction> getViewActions(Map<DatamartViewPair, DatamartViewWrap> viewMap,
                                                   List<ViewReplaceAction> actions) {
        return actions.stream()
                .filter(a -> viewMap.containsKey(a.getViewPair()))
                .peek(a -> {
                    DatamartViewWrap wrap = viewMap.get(a.getViewPair());
                    SqlNode sqlNode = definitionService.processingQuery(wrap.getView().getQuery());
                    a.setTo(sqlNode);
                })
                .collect(toList());
    }

    public void processSqlSelect(SqlSelect sqlSelect, List<ViewReplaceAction> actions) {
        processSqlNode(sqlSelect.getSelectList(), false, actions, ignore());
        processSqlNode(sqlSelect.getFrom(), true
                , actions, node -> actions.add(new ViewReplaceAction(node, true, sqlSelect::setFrom)));
        processSqlNode(sqlSelect.getWhere(), false, actions, ignore());
    }

    @NotNull
    private Handler<SqlNode> ignore() {
        return ar -> {
        };
    }

    private void processSqlNode(SqlNode node,
                                boolean replacementPossible,
                                List<ViewReplaceAction> actions,
                                Handler<SqlNode> handler) {
        if (node instanceof SqlSelect) {
            processSqlSelect((SqlSelect) node, actions);
        } else if (node instanceof SqlIdentifier) {
            if (replacementPossible) {
                handler.handle(node);
            }
        } else if (node instanceof SqlJoin) {
            processSqlJoin((SqlJoin) node, actions);
        } else if (node instanceof SqlSnapshot) {
            handler.handle(node);
        } else if (node instanceof SqlBasicCall) {
            processSqlBasicCall((SqlBasicCall) node, replacementPossible, actions);
        } else if (node instanceof SqlNodeList) {
            val nodeList = ((SqlNodeList) node).getList();
            nodeList.forEach(listNode -> processSqlNode(listNode, false, actions, ignore()));
        } else if (node instanceof SqlCall) {
            val nodeList = ((SqlCall) node).getOperandList();
            nodeList.forEach(listNode -> processSqlNode(listNode, false, actions, ignore()));
        }
    }

    private void processSqlBasicCall(SqlBasicCall basicCall,
                                     boolean replacementPossible,
                                     List<ViewReplaceAction> actions) {
        for (int i = 0; i < basicCall.getOperands().length; i++) {
            val nodePos = i;
            processSqlNode(basicCall.operand(i), replacementPossible, actions, node -> {
                if (replacementPossible && nodePos == 0) {
                    actions.add(new ViewReplaceAction(node, (n) -> basicCall.setOperand(nodePos, n)));
                }
            });
        }
    }

    private void processSqlJoin(SqlJoin join,
                                List<ViewReplaceAction> actions) {
        processSqlNode(join.getLeft(), true, actions
                , node -> actions.add(new ViewReplaceAction(node, true, join::setLeft)));
        processSqlNode(join.getRight(), true, actions
                , node -> actions.add(new ViewReplaceAction(node, true, join::setRight)));
    }

}


