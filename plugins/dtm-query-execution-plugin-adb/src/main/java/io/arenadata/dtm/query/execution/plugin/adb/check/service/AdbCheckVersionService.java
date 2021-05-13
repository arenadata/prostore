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
package io.arenadata.dtm.query.execution.plugin.adb.check.service;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbVersionInfoFactory;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbVersionQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.impl.AdbVersionQueriesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("adbCheckVersionService")
public class AdbCheckVersionService implements CheckVersionService {

    private final DatabaseExecutor databaseExecutor;
    private final AdbVersionQueriesFactory versionQueriesFactory;
    private final AdbVersionInfoFactory versionInfoFactory;
    private final List<ColumnMetadata> metadata;

    @Autowired
    public AdbCheckVersionService(DatabaseExecutor databaseExecutor,
                                  AdbVersionQueriesFactory versionQueriesFactory,
                                  AdbVersionInfoFactory versionInfoFactory) {
        this.databaseExecutor = databaseExecutor;
        this.versionQueriesFactory = versionQueriesFactory;
        this.versionInfoFactory = versionInfoFactory;
        metadata = createColumnMetadata();
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return Future.future(promise -> {
            CompositeFuture.join(Arrays.asList(databaseExecutor.execute(versionQueriesFactory.createAdbVersionQuery(), metadata),
                    databaseExecutor.execute(versionQueriesFactory.createFdwVersionQuery(), metadata),
                    databaseExecutor.execute(versionQueriesFactory.createPxfVersionQuery(), metadata)))
                    .onSuccess(result -> {
                        List<List<Map<String, Object>>> list = result.list();
                        List<Map<String, Object>> resultList = list.stream()
                                .flatMap(List::stream)
                                .collect(Collectors.toList());
                        promise.complete(versionInfoFactory.create(resultList));
                    })
                    .onFailure(promise::fail);
        });
    }

    private List<ColumnMetadata> createColumnMetadata() {
        return Arrays.asList(ColumnMetadata.builder()
                        .name(AdbVersionQueriesFactoryImpl.COMPONENT_NAME_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build(),
                ColumnMetadata.builder()
                        .name(AdbVersionQueriesFactoryImpl.VERSION_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build()
        );
    }
}
