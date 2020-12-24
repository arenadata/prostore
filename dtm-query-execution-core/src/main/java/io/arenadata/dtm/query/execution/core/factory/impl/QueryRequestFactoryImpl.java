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
package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.factory.QueryRequestFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class QueryRequestFactoryImpl implements QueryRequestFactory {

    private final AppConfiguration configuration;

    @Autowired
    public QueryRequestFactoryImpl(AppConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public QueryRequest create(InputQueryRequest inputQueryRequest) {
        return QueryRequest.builder()
                .requestId(inputQueryRequest.getRequestId())
                .datamartMnemonic(inputQueryRequest.getDatamartMnemonic())
                .sql(inputQueryRequest.getSql())
                .parameters(inputQueryRequest.getParameters())
                .envName(configuration.getEnvName())
                .build();
    }
}
