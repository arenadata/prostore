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
package io.arenadata.dtm.query.execution.plugin.api.dml;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.val;

import java.util.List;
import java.util.Map;

public class LlrEstimateUtils {
    public static final ColumnMetadata LLR_ESTIMATE_METADATA = new ColumnMetadata("plan", ColumnType.ANY);

    private LlrEstimateUtils() {
    }

    public static String prepareResultJson(SourceType sourceType, String enrichedQuery, String plan) {
        try {
            val mapper = DatabindCodec.mapper();
            val planJson = plan != null ? mapper.readTree(plan) : null;
            val normalizedEnrichedQuery = normalize(enrichedQuery);
            return mapper.writeValueAsString(new LlrEstimateResult(sourceType, planJson, normalizedEnrichedQuery));
        } catch (Exception e) {
            throw new DtmException("Could not prepare estimate result JSON", e);
        }
    }

    public static String extractPlanJson(List<Map<String, Object>> resultSet) {
        if (resultSet == null || resultSet.size() != 1) {
            return null;
        }

        Map<String, Object> item = resultSet.get(0);
        String columnName = LlrEstimateUtils.LLR_ESTIMATE_METADATA.getName();
        if (!item.containsKey(columnName)) {
            return null;
        }

        Object o = item.get(columnName);
        return o.toString();
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }

        return value.replaceAll("\r\n|\r|\n", " ")
                .replaceAll("\\s+", " ");
    }
}
