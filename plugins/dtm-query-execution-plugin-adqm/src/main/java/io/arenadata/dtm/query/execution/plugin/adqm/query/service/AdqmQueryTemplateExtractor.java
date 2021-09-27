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
package io.arenadata.dtm.query.execution.plugin.adqm.query.service;

import io.arenadata.dtm.calcite.adqm.extension.dml.LimitableSqlOrderBy;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import lombok.val;
import lombok.var;
import org.apache.calcite.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;

@Service("adqmQueryTemplateExtractor")
public class AdqmQueryTemplateExtractor extends AbstractQueryTemplateExtractor {

    @Autowired
    public AdqmQueryTemplateExtractor(@Qualifier("adqmCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                      @Qualifier("adqmSqlDialect") SqlDialect sqlDialect) {
        super(definitionService, sqlDialect);
    }

    @Override
    public SqlNode enrichTemplate(EnrichmentTemplateRequest request) {
        SqlNode sqlNode = SqlNodeUtil.copy(request.getTemplateNode());
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);

        var holdParameters = 0;
        if (sqlNode.getClass() == SqlSelect.class) {
            SqlNode from = ((SqlSelect) sqlNode).getFrom();
            if (from.getClass() == SqlBasicCall.class) {
                SqlNode operand = ((SqlBasicCall) from).getOperands()[0];
                if (operand.getClass() == LimitableSqlOrderBy.class) {
                    LimitableSqlOrderBy limitableSqlOrderBy = (LimitableSqlOrderBy) operand;
                    if (limitableSqlOrderBy.fetch != null && limitableSqlOrderBy.fetch.getClass() == SqlDynamicParam.class) {
                        holdParameters++;
                    }
                    if (limitableSqlOrderBy.offset != null && limitableSqlOrderBy.offset.getClass() == SqlDynamicParam.class) {
                        holdParameters++;
                    }
                }
            }
        }

        List<SqlTreeNode> dynamicNodes = selectTree.findNodes(DYNAMIC_PARAM_PREDICATE, true);
        // parameters encounter twice excluding fetch/offset ones
        if (dynamicNodes.size() != (request.getParams().size() - holdParameters) * 2 + holdParameters) {
            throw new DtmException("The number of passed parameters and parameters in the template does not match");
        }

        Iterator<SqlNode> paramIterator = request.getParams().subList(0, request.getParams().size() - holdParameters).iterator();
        for (int i = 0; i < dynamicNodes.size() - holdParameters; i++) {
            val dynamicNode = dynamicNodes.get(i);
            SqlNode param;
            if (!paramIterator.hasNext()) {
                paramIterator = request.getParams().iterator();
            }
            param = paramIterator.next();
            dynamicNode.getSqlNodeSetter().accept(param);
        }

        for (int i = holdParameters; i > 0; i--) {
            val dynamicNode = dynamicNodes.get(dynamicNodes.size() - i);
            val param = request.getParams().get(request.getParams().size() - i);
            dynamicNode.getSqlNodeSetter().accept(param);
        }

        return selectTree.getRoot().getNode();
    }
}
