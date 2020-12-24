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

import io.arenadata.dtm.query.execution.core.dto.dml.SnapshotReplaceAction;
import io.arenadata.dtm.query.execution.core.service.dml.SqlSnapshotReplacer;
import io.vertx.core.Handler;
import lombok.val;
import org.apache.calcite.sql.*;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class SqlSnapshotReplacerImpl implements SqlSnapshotReplacer {

    @Override
    public void replace(SqlSnapshot parentSnapshot, SqlSelect replacingNode) {
        val actions = new ArrayList<SnapshotReplaceAction>();
        processSqlSelect(replacingNode, actions);
        for (int i = actions.size() - 1; i >= 0; i--) {
            SnapshotReplaceAction replaceAction = actions.get(i);
            replaceAction.setTo(createSqlSnapshot(parentSnapshot, replaceAction.getFrom()));
            replaceAction.run();
        }
    }


    public void processSqlSelect(SqlSelect sqlSelect, List<SnapshotReplaceAction> actions) {
        processSqlNode(sqlSelect.getSelectList(), false, actions, ignore());
        processSqlNode(sqlSelect.getFrom(), true
                , actions, ar -> actions.add(new SnapshotReplaceAction(ar, sqlSelect::setFrom)));
        processSqlNode(sqlSelect.getWhere(), false, actions, ignore());
    }

    @NotNull
    private Handler<SqlIdentifier> ignore() {
        return ar -> {
        };
    }

    private void processSqlNode(SqlNode node,
                                boolean replacementPossible,
                                List<SnapshotReplaceAction> actions,
                                Handler<SqlIdentifier> handler) {
        if (node instanceof SqlSelect) {
            processSqlSelect((SqlSelect) node, actions);
        } else if (node instanceof SqlIdentifier) {
            if (replacementPossible) {
                handler.handle((SqlIdentifier) node);
            }
        } else if (node instanceof SqlJoin) {
            processSqlJoin((SqlJoin) node, actions);
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
                                     List<SnapshotReplaceAction> actions) {
        for (int i = 0; i < basicCall.getOperands().length; i++) {
            val nodePos = i;
            processSqlNode(basicCall.operand(i), replacementPossible, actions, node -> {
                if (replacementPossible && nodePos == 0) {
                    actions.add(new SnapshotReplaceAction(node, (n) -> basicCall.setOperand(nodePos, n)));
                }
            });
        }
    }

    private void processSqlJoin(SqlJoin join,
                                List<SnapshotReplaceAction> actions) {
        processSqlNode(join.getLeft(), true, actions
                , node -> actions.add(new SnapshotReplaceAction(node, join::setLeft)));
        processSqlNode(join.getRight(), true, actions
                , node -> actions.add(new SnapshotReplaceAction(node, join::setRight)));
    }

    private SqlSnapshot createSqlSnapshot(SqlSnapshot parentSnapshot, SqlIdentifier target) {
        return new SqlSnapshot(target.getParserPosition(), target, parentSnapshot.getPeriod());
    }

}
