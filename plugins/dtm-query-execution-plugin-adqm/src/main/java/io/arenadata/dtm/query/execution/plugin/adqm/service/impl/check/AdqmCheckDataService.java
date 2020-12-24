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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service("adqmCheckDataService")
public class AdqmCheckDataService implements CheckDataService {
    private static final String COUNT = "count";
    private static final String COUNT_QUERY_PATTERN = "  SELECT count(1) as %s\n" +
            "  FROM %s__%s.%s_actual FINAL\n" +
            "  WHERE (sys_to = %s AND sys_op = 1) OR sys_from = %s";

    private static final String SUM = "sum";
    private static final String HASH_QUERY_PATTERN = "SELECT \n" +
            "  sum(\n" +
            "    reinterpretAsUInt32(\n" +
            "      lower(\n" +
            "        hex(\n" +
            "          MD5(\n" +
            "            %s\n" +
            "          )\n" +
            "        )\n" +
            "      )\n" +
            "    )\n" +
            "  ) as %s\n" +
            "FROM %s__%s.%s_actual FINAL\n" +
            "WHERE (sys_to = %s AND sys_op = 1) OR sys_from = %s";

    private final DatabaseExecutor adqmQueryExecutor;

    @Autowired
    public AdqmCheckDataService(DatabaseExecutor adqmQueryExecutor) {
        this.adqmQueryExecutor = adqmQueryExecutor;
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountParams params) {
        Entity entity = params.getEntity();
        String query = String.format(COUNT_QUERY_PATTERN, COUNT, params.getEnv(), entity.getSchema(),
                entity.getName(), params.getSysCn() - 1, params.getSysCn());
        return adqmQueryExecutor.execute(query)
                .map(result -> Long.parseLong(result.get(0).get(COUNT).toString()));
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Params params) {
        Entity entity = params.getEntity();
        List<String> columns = params.getColumns().stream()
                .map(column -> String.format("ifNull(toString(%s),'')", column))
                .collect(Collectors.toList());
        String colQuery = columns.size() > 1
                ? String.format("concat(%s)", String.join(",';',", columns))
                : columns.stream().findFirst().get();

        String query = String.format(HASH_QUERY_PATTERN, colQuery, SUM, params.getEnv(), entity.getSchema(),
                entity.getName(), params.getSysCn() - 1, params.getSysCn());
        return adqmQueryExecutor.execute(query)
                .map(result -> Long.parseLong(result.get(0).get(SUM).toString()));
    }
}
