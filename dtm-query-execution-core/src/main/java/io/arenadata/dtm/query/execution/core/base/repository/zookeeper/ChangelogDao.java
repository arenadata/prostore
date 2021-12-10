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
package io.arenadata.dtm.query.execution.core.base.repository.zookeeper;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Changelog;
import io.arenadata.dtm.common.model.ddl.DenyChanges;
import io.arenadata.dtm.query.execution.core.base.repository.DaoUtils;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static io.arenadata.dtm.query.execution.core.base.repository.DaoUtils.serialize;

@Repository
public class ChangelogDao {
    private static final String PREVIOUS_NOT_COMPLETED = "Previous change operation is not completed in datamart [%s]";
    private static final String CHANGE_OPERATIONS_ARE_FORBIDDEN = "Change operations are forbidden";
    private static final String COULD_NOT_CREATE_CHANGELOG = "Changelog node already exist";
    private final ZookeeperExecutor executor;
    private final String envPath;

    public ChangelogDao(@Qualifier("zookeeperExecutor") ZookeeperExecutor executor,
                        @Value("${core.env.name}") String systemName) {
        this.executor = executor;
        this.envPath = "/" + systemName;
    }

    public Future<Void> writeNewRecord(String datamart, String entityName, String changeQuery, OkDelta deltaOk) {
        return Future.future(promise -> {
            val datamartPath = String.format("%s/%s", envPath, datamart);
            val changelogPath = String.format("%s/changelog", datamartPath);
            val changelogStat = new Stat();
            val deltaNum = deltaOk == null ? null : deltaOk.getDeltaNum();
            executor.getData(changelogPath, null, changelogStat)
                    .compose(Future::succeededFuture, throwable -> createNewChangelogIfNotPresent(changelogPath))
                    .map(bytes -> checkChangeQuery(bytes, changeQuery, deltaNum, datamart))
                    .compose(changelog -> changelog != null ? Future.succeededFuture() : setData(datamartPath, changelogPath, entityName,
                            changeQuery, deltaNum, changelogStat, datamart))
                    .onComplete(promise);
         });
    }

    public Future<List<Changelog>> getChanges(String datamart) {
        val changelogPath = String.format("%s/%s/changelog", envPath, datamart);
        return executor.getChildren(changelogPath)
                .compose(children -> getChanges(changelogPath, children, datamart))
                .compose(changelogs -> executor.getData(changelogPath)
                        .map(bytes -> {
                            if (bytes == null) {
                                return changelogs;
                            }

                            val parentChangelog = DaoUtils.deserialize(bytes, datamart, Changelog.class);
                            parentChangelog.setOperationNumber((long) changelogs.size());
                            List<Changelog> result = new ArrayList<>(changelogs);
                            result.add(parentChangelog);
                            return result;
                        })
                )
                .otherwise(e -> {
                    if (e instanceof KeeperException.NoNodeException) {
                        throw new DtmException(String.format("Node %s not found", changelogPath), e);
                    }

                    throw new DtmException("Unexpected exception during getChanges", e);
                });

    }

    private Future<List<Changelog>> getChanges(String changelogPath, List<String> changelogChildren, String datamart) {
        List<Future> changes = new ArrayList<>();
        changelogChildren.forEach(child -> changes.add(getChangelog(changelogPath, child, datamart)));
        return CompositeFuture.join(changes)
                .map(CompositeFuture::list);
    }

    private Future<Changelog> getChangelog(String changelogPath, String childPath, String datamart) {
        return executor.getData(changelogPath + "/" + childPath)
                .map(bytes -> DaoUtils.deserialize(bytes, datamart, Changelog.class))
                .map(changelog -> {
                    changelog.setOperationNumber(Long.parseLong(childPath));
                    return changelog;
                });
    }

    private Future<byte[]> createNewChangelogIfNotPresent(String changelogPath) {
        return executor.create(changelogPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
                .otherwise(e -> {
                    if (e instanceof KeeperException.NodeExistsException) {
                        throw new DtmException(COULD_NOT_CREATE_CHANGELOG, e);
                    }

                    throw new DtmException("Unexpected exception during writeNewRecord", e);
                })
                .mapEmpty();
    }

    private Changelog checkChangeQuery(byte[] bytes, String changeQuery, Long deltaNum, String datamart) {
        if (bytes == null) {
            return null;
        }

        val changelog = DaoUtils.deserialize(bytes, datamart, Changelog.class);
        if (!Objects.equals(changeQuery, changelog.getChangeQuery()) ||
                !Objects.equals(deltaNum, changelog.getDeltaNum())) {
            throw new DtmException(String.format(PREVIOUS_NOT_COMPLETED, datamart));
        }

        return changelog;
    }

    private Future<Void> setData(String datamartPath, String changelogPath, String entityName, String changeQuery,
                                 Long deltaNum, Stat changelogStat, String datamart) {
        val changelog = Changelog.builder()
                .changeQuery(changeQuery)
                .dateTimeStart(DaoUtils.getCurrentDateTime())
                .entityName(entityName)
                .deltaNum(deltaNum)
                .build();
        val denyChanges = new DenyChanges(null, null);

        return executor.multi(Arrays.asList(
                        Op.setData(changelogPath, serialize(changelog), changelogStat.getVersion()),
                        Op.create(datamartPath + "/block", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                        Op.create(datamartPath + "/immutable", serialize(denyChanges), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
                ))
                .otherwise(e -> {
                    if (e instanceof KeeperException.NodeExistsException) {
                        throw new DtmException(CHANGE_OPERATIONS_ARE_FORBIDDEN, e);
                    } else if (e instanceof KeeperException.BadVersionException) {
                        throw new DtmException(String.format(PREVIOUS_NOT_COMPLETED, datamart), e);
                    }

                    throw new DtmException("Unexpected exception during writeNewRecord", e);
                })
                .mapEmpty();
    }
}
