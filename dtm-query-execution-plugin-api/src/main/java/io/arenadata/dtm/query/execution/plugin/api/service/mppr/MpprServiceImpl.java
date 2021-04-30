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
package io.arenadata.dtm.query.execution.plugin.api.service.mppr;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.vertx.core.Future;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MpprServiceImpl<T extends MpprExecutor> implements MpprService {
    private final Map<ExternalTableLocationType, MpprExecutor> executors;

    public MpprServiceImpl(List<T> executors) {
        this.executors = executors.stream()
                .collect(Collectors.toMap(MpprExecutor::getType, Function.identity()));
    }

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        return executors.get(request.getExternalTableLocationType()).execute(request);
    }
}
