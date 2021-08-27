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
package io.arenadata.dtm.query.execution.plugin.adg.base.service.client;

import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.OperationFile;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request.*;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.response.ResOperation;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.response.TtLoadDataKafkaResponse;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema.Space;
import io.arenadata.dtm.query.execution.plugin.adg.rollback.dto.ReverseHistoryTransferRequest;
import io.vertx.core.Future;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * REST-client for connecting with Tarantool Cartridge
 */
public interface AdgCartridgeClient {

    Future<ResOperation> getFiles();

    Future<ResOperation> setFiles(List<OperationFile> files);

    Future<ResOperation> getSchema();

    Future<ResOperation> setSchema(String yaml);

    Future<Void> uploadData(AdgUploadDataKafkaRequest request);

    Future<Void> subscribe(AdgSubscriptionKafkaRequest request);

    Future<TtLoadDataKafkaResponse> loadData(AdgLoadDataKafkaRequest request);

    Future<Void> transferDataToScdTable(AdgTransferDataEtlRequest request);

    Future<Void> cancelSubscription(String topicName);

    Future<Void> reverseHistoryTransfer(ReverseHistoryTransferRequest request);

    Future<Void> executeCreateSpacesQueued(OperationYaml request);

    Future<Void> executeDeleteSpacesQueued(AdgDeleteTablesRequest request);

    Future<Void> executeDeleteSpacesWithPrefixQueued(AdgDeleteTablesWithPrefixRequest request);

    Future<Map<String, Space>> getSpaceDescriptions(Set<String> spaceNames);

    Future<Long> getCheckSumByInt32Hash(String actualDataTableName,
                                        String historicalDataTableName,
                                        Long sysCn,
                                        Set<String> columnList,
                                        Long normalization);

    Future<Void> deleteSpaceTuples(String spaceName, String whereCondition);

    Future<Void> truncateSpace(String spaceName);

    Future<List<VersionInfo>> getCheckVersions();
}
