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

import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.List;

public class NullNotCastableRelToSqlConverter extends RelToSqlConverter {

    public NullNotCastableRelToSqlConverter(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    public Result visit(Project e) {
        e.getVariablesSet();
        Result x = visitChild(0, e.getInput());
        parseCorrelTable(e, x);
        if (isStar(e.getChildExps(), e.getInput().getRowType(), e.getRowType())) {
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

    protected void parseCorrelTable(RelNode relNode, Result x) {
        for (CorrelationId id : relNode.getVariablesSet()) {
            correlTableMap.put(id, x.qualifiedContext());
        }
    }

    @Override
    public Result visit(RelNode e) {
        if (e instanceof EnumerableLimit) {
            e.getVariablesSet();
            RelNode input = e.getInput(0);
            if (input instanceof EnumerableSort) {
                val node = (EnumerableSort) input;
                val sort = EnumerableSort.create(node.getInput(),
                        node.getCollation(),
                        ((EnumerableLimit) e).offset,
                        ((EnumerableLimit) e).fetch);
                return visitChild(0, sort);
            } else {
                Result x = visitChild(0, input);
                parseCorrelTable(e, x);
                final Builder builder = x.builder(input, Clause.SELECT, Clause.FETCH, Clause.OFFSET);
                setFetch(builder, e);
                setOffset(builder, e);
                return builder.result();
            }
        } else {
            throw new AssertionError("Need to implement " + e.getClass().getName());
        }
    }

    @Override
    public void addSelect(List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
        String name = rowType.getFieldNames().get(selectList.size());
        String alias = SqlValidatorUtil.getAlias(node, -1);
        if (alias == null || !alias.equals(name)) {
            if (name.startsWith("$")) {
                name = name.substring(1);
            }
            node = as(node, name);
        }
        selectList.add(node);
    }

    private void setOffset(Builder builder, RelNode node) {
        RexNode offset = ((EnumerableLimit) node).offset;
        if (offset != null) {
            builder.setOffset(SqlLiteral.createExactNumeric(offset.toStringRaw(), POS));
        }
    }

    private void setFetch(Builder builder, RelNode node) {
        RexNode fetch = ((EnumerableLimit) node).fetch;
        if (fetch != null) {
            builder.setFetch(SqlLiteral.createExactNumeric(fetch.toStringRaw(), POS));
        }
    }

    @Override
    protected boolean isAnon() {
        return false;
    }
}
