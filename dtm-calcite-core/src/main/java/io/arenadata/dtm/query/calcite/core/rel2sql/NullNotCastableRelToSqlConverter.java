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
package io.arenadata.dtm.query.calcite.core.rel2sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.List;

public class NullNotCastableRelToSqlConverter extends RelToSqlConverter {
    private final boolean allowStarInProject;

    public NullNotCastableRelToSqlConverter(SqlDialect dialect, boolean allowStarInProject) {
        super(dialect);
        this.allowStarInProject = allowStarInProject;
    }

    @Override
    public Result visit(Project e) {
        e.getVariablesSet();
        Result x = visitChild(0, e.getInput());
        parseCorrelationTable(e, x);

        if (allowStarInProject && isStar(e.getChildExps(), e.getInput().getRowType(), e.getRowType())) {
            return x;
        }

        final Builder builder = x.builder(e, Clause.SELECT);
        final List<SqlNode> selectList = new ArrayList<>();
        for (RexNode ref : e.getChildExps()) {
            SqlNode sqlExpr = builder.context.toSql(null, ref);
            addSelect(selectList, sqlExpr, e.getRowType());
        }
        builder.setSelect(new SqlNodeList(selectList, POS));
        return builder.result();
    }

    @Override
    public Result visit(Filter e) {
        Result visit = super.visit(e);
        parseCorrelationTable(e, visit);
        if(allowStarInProject) {
            return visit;
        }

        Builder builder = visit.builder(e, Clause.SELECT);
        final List<SqlNode> selectList = new ArrayList<>();
        RexBuilder rexBuilder = e.getCluster().getRexBuilder();
        for (int i = 0; i < e.getRowType().getFieldCount(); i++) {
            RexInputRef columnRex = rexBuilder.makeInputRef(e, i);
            SqlNode sqlExpr = builder.context.toSql(null, columnRex);
            addSelect(selectList, sqlExpr, e.getRowType());
        }
        builder.setSelect(new SqlNodeList(selectList, POS));
        return builder.result();
    }

    private void parseCorrelationTable(RelNode relNode, Result x) {
        for (CorrelationId id : relNode.getVariablesSet()) {
            correlTableMap.put(id, x.qualifiedContext());
        }
    }

    @Override
    public void addSelect(List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
        String name = rowType.getFieldNames().get(selectList.size());
        String alias = SqlValidatorUtil.getAlias(node, -1);
        if (alias == null || !alias.equals(name)) {
            selectList.add(as(node, name));
            return;
        }

        selectList.add(node);
    }

    @Override
    protected boolean isAnon() {
        return false;
    }
}
