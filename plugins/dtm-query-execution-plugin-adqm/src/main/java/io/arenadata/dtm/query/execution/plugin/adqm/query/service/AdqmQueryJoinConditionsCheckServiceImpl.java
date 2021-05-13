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

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adqm.query.dto.AdqmCheckJoinRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.query.dto.AdqmJoinQuery;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.extractor.SqlJoinConditionExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AdqmQueryJoinConditionsCheckServiceImpl implements AdqmQueryJoinConditionsCheckService {

    private final SqlJoinConditionExtractor joinConditionExtractor;

    @Autowired
    public AdqmQueryJoinConditionsCheckServiceImpl(SqlJoinConditionExtractor joinConditionExtractor) {
        this.joinConditionExtractor = joinConditionExtractor;
    }

    @Override
    public boolean isJoinConditionsCorrect(AdqmCheckJoinRequest request) {
        try {
            List<AdqmJoinQuery> queryJoins = joinConditionExtractor.extract(request.getRelNode());
            Map<String, Map<Integer, Integer>> tableDistrKeyMap = new HashMap<>();
            request.getSchema().forEach(d -> {
                String schema = d.getMnemonic();
                tableDistrKeyMap.putAll(d.getEntities().stream().collect(Collectors.toMap(e -> getTableWithSchema(schema, e.getName()),
                        e -> e.getFields().stream()
                                .filter(f -> f.getShardingOrder() != null)
                                .collect(Collectors.toMap(EntityField::getOrdinalPosition, EntityField::getShardingOrder))
                )));
            });

            for (AdqmJoinQuery join : queryJoins) {
                //TODO implement checking conditions with more than one join
                if (join.getLeft() instanceof LogicalTableScan
                        && join.getRight() instanceof LogicalTableScan) {
                    if (!isJoinEquiConditionsCorrect(join, tableDistrKeyMap)) {
                        return false;
                    }
                } else {
                    throw new DtmException("Unsupported sql join node type");
                }
            }
            return true;
        } catch (Exception e) {
            log.error("Error in checking join conditions", e);
            throw new DtmException(e);
        }
    }

    private boolean isJoinEquiConditionsCorrect(AdqmJoinQuery join, Map<String, Map<Integer, Integer>> tableDistrKeyMap) {
        if (join.getJoinInfo().nonEquiConditions.isEmpty()
                && (!join.getJoinInfo().leftKeys.isEmpty() || !join.getJoinInfo().rightKeys.isEmpty())) {
            int distrKeyCount = 0;
            for (int i = 0; i < join.getJoinInfo().leftKeys.size(); i++) {
                Integer lKey = join.getJoinInfo().leftKeys.get(i);
                Integer rKey = join.getJoinInfo().rightKeys.get(i);

                Integer lDistrId = tableDistrKeyMap.get(getTableWithSchema(join.getLeft().getTable().getQualifiedName())).get(lKey);
                Integer rDistrId = tableDistrKeyMap.get(getTableWithSchema(join.getRight().getTable().getQualifiedName())).get(rKey);
                if (lDistrId != null && lDistrId.equals(rDistrId)) {
                    distrKeyCount++;
                }
            }
            return tableDistrKeyMap.get(getTableWithSchema(join.getLeft().getTable().getQualifiedName())).size() == distrKeyCount;
        } else {
            return false;
        }
    }

    private static String getTableWithSchema(String schema, String name) {
        return schema + "." + name;
    }

    private static String getTableWithSchema(List<String> names) {
        return getTableWithSchema(names.get(0), names.get(1));
    }
}
