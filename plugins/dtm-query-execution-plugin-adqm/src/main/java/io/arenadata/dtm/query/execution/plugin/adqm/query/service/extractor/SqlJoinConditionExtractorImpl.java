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
package io.arenadata.dtm.query.execution.plugin.adqm.query.service.extractor;

import io.arenadata.dtm.query.execution.plugin.adqm.query.dto.AdqmJoinQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class SqlJoinConditionExtractorImpl implements SqlJoinConditionExtractor {

    @Override
    public List<AdqmJoinQuery> extract(RelNode relNode) {
        List<AdqmJoinQuery> joins = new ArrayList<>();
        getLogicalJoins(relNode, joins);
        return joins;
    }

    private void getLogicalJoins(RelNode relNode, List<AdqmJoinQuery> joins) {
        relNode.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(LogicalJoin join) {
                joins.add(new AdqmJoinQuery(join.getLeft(),
                        join.getRight(),
                        join.analyzeCondition(),
                        (RexCall) join.getCondition()));
                return super.visit(join);
            }

            /*@Override
            protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                getLogicalJoins(child, joins);
                return super.visitChild(parent, i, child);
            }*/
        });

    }
}
