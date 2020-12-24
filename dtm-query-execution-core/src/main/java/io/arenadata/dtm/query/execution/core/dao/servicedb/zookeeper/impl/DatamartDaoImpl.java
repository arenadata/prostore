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
package io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl;

import io.arenadata.dtm.async.AsyncUtils;
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dto.delta.Delta;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartInfo;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class DatamartDaoImpl implements DatamartDao {
    private static final int CREATE_DATAMART_OP_INDEX = 0;
    private static final byte[] EMPTY_DATA = null;
    private final ZookeeperExecutor executor;
    private final String envPath;

    public DatamartDaoImpl(ZookeeperExecutor executor, @Value("${core.env.name}") String systemName) {
        this.executor = executor;
        envPath = "/" + systemName;
    }

    @Override
    public Future<Void> createDatamart(String name) {
        return executor.createEmptyPersistentPath(envPath)
            .otherwise(error -> {
                if (error instanceof KeeperException.NodeExistsException) {
                    return envPath;
                } else {
                    throw error(error,
                        String.format("Can't create datamart [%s]", name),
                        RuntimeException::new);
                }
            })
            .compose(r -> executor.multi(getCreateDatamartOps(getTargetPath(name))))
            .otherwise(error -> {
                if (error instanceof KeeperException.NodeExistsException) {
                    if (isDatamartExists((KeeperException) error)) {
                        throw error(error,
                            String.format("Datamart [%s] already exists!", name),
                            DatamartAlreadyExistsException::new);
                    }
                }
                throw error(error,
                    String.format("Can't create datamart [%s]", name),
                    RuntimeException::new);
            })
            .compose(AsyncUtils::toEmptyVoidFuture)
            .onSuccess(s -> log.info("Datamart [{}] successfully created", name));
    }

    private List<Op> getCreateDatamartOps(String datamartPath) {
        byte[] deltaData;
        try {
            deltaData = DatabindCodec.mapper().writeValueAsBytes(new Delta());
        } catch (Exception ex) {
            throw new RuntimeException("Can't serialize delta");
        }
        return Arrays.asList(
            Op.create(datamartPath, EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            createDatamartNodeOp(datamartPath, "/entity"),
            Op.create(datamartPath + "/delta", deltaData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            createDatamartNodeOp(datamartPath, "/delta/num"),
            createDatamartNodeOp(datamartPath, "/delta/date")
        );
    }

    private Op createDatamartNodeOp(String datamartPath, String nodeName) {
        return Op.create(datamartPath + nodeName, EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private boolean isDatamartExists(KeeperException error) {
        List<OpResult> results = error.getResults() == null ? Collections.emptyList() : error.getResults();
        return results.size() > 0 && results.get(CREATE_DATAMART_OP_INDEX) instanceof OpResult.ErrorResult;
    }

    @Override
    public void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler) {
        getDatamarts()
            .onSuccess(names -> resultHandler.handle(
                Future.succeededFuture(
                    names.stream()
                        .map(DatamartInfo::new)
                        .collect(Collectors.toList()
                        )
                )))
            .onFailure(error -> resultHandler.handle(Future.failedFuture(error)));
    }

    @Override
    public Future<List<String>> getDatamarts() {
        return executor.getChildren(envPath)
            .otherwise(error -> {
                if (error instanceof KeeperException.NoNodeException) {
                    throw error(error,
                        String.format("Env [%s] not exists", envPath),
                        RuntimeException::new);
                } else {
                    throw error(error,
                        "Can't get datamarts",
                        RuntimeException::new);
                }
            });
    }

    @Override
    public Future<?> getDatamart(String name) {
        return executor.getData(getTargetPath(name))
            .otherwise(error -> {
                if (error instanceof KeeperException.NoNodeException) {
                    throw error(error,
                        String.format("Datamart [%s] not exists", name),
                        DatamartNotExistsException::new);
                } else {
                    throw error(error,
                        String.format("Can't get datamarts [%s]", name),
                        RuntimeException::new);
                }
            });
    }

    @Override
    public Future<Boolean> existsDatamart(String name) {
        return executor.exists(getTargetPath(name));
    }

    @Override
    public Future<Void> deleteDatamart(String name) {
        return executor.deleteRecursive(getTargetPath(name))
            .otherwise(error -> {
                if (error instanceof IllegalArgumentException) {
                    throw error(error,
                        String.format("Datamart [%s] does not exists!", name),
                        DatamartNotExistsException::new);
                } else {
                    throw error(error,
                        String.format("Can't delete datamarts [%s]", name),
                        RuntimeException::new);
                }
            })
            .onSuccess(s -> log.info("Datamart [{}] successfully removed", name));
    }

    private RuntimeException error(Throwable error,
                                   String errMsg,
                                   BiFunction<String, Throwable, RuntimeException> errFunc) {
        log.error(errMsg, error);
        return errFunc.apply(errMsg, error);
    }

    @Override
    public String getTargetPath(String target) {
        return String.format("%s/%s", envPath, target);
    }
}
