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
package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Determining the type of request by hint DATASOURCE_TYPE = 'ADB|ADG|ADQM'
 */
@Slf4j
@Component
public class HintExtractor {
    private final Pattern HINT_PATTERN = Pattern.compile(
        "DATASOURCE_TYPE\\s*=\\s*'([^\\s]+)'\\s*$",
        Pattern.CASE_INSENSITIVE);

    public QuerySourceRequest extractHint(QueryRequest request) {
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        Matcher matcher = HINT_PATTERN.matcher(request.getSql());
        if (matcher.find()) {
            String dataSource = matcher.group(1);
            String strippedSql = matcher.replaceFirst(StringUtils.EMPTY).trim();
            QueryRequest newQueryRequest = request.copy();
            newQueryRequest.setSql(strippedSql);
            sourceRequest.setSourceType(SourceType.valueOfAvailable(dataSource));
            sourceRequest.setQueryRequest(newQueryRequest);
        } else {
            log.info("Hint not defined for request {}", request.getSql());
            sourceRequest.setQueryRequest(request);
        }
        return sourceRequest;
    }
}
