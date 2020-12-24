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
package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeleteWriteOperationExecutor;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DeleteWriteOperationExecutorImpl extends WriteOperationSuccessExecutorImpl implements DeleteWriteOperationExecutor {

    public DeleteWriteOperationExecutorImpl(ZookeeperExecutor executor,
                                               @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return DeleteWriteOperationExecutor.class;
    }
}
