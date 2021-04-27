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
package io.arenadata.dtm.query.execution.core.query.service.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.PreparedQueryKey;
import io.arenadata.dtm.common.cache.PreparedQueryValue;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.query.exception.PreparedStatementNotFoundException;
import io.arenadata.dtm.query.execution.core.query.service.QueryPreparedService;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class QueryPreparedServiceImpl implements QueryPreparedService {

    private final CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService;

    @Autowired
    public QueryPreparedServiceImpl(@Qualifier("corePreparedQueryCacheService")
                                            CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService) {
        this.preparedQueryCacheService = preparedQueryCacheService;
    }

    @Override
    public SqlNode getPreparedQuery(QueryRequest request) {
        PreparedQueryValue preparedQueryValue = preparedQueryCacheService.get(new PreparedQueryKey(request.getSql()));
        if (preparedQueryValue != null) {
            return preparedQueryValue.getSqlNode();
        } else {
            throw new PreparedStatementNotFoundException();
        }
    }
}
