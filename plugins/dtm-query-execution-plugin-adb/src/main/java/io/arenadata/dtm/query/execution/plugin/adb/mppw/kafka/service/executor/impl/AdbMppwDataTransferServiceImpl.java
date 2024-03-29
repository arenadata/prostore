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
package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.impl;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AdbMppwDataTransferServiceImpl implements AdbMppwDataTransferService {

    private final MppwRequestFactory<AdbKafkaMppwTransferRequest> mppwRequestFactory;
    private final DatabaseExecutor adbQueryExecutor;

    @Autowired
    public AdbMppwDataTransferServiceImpl(MppwRequestFactory<AdbKafkaMppwTransferRequest> mppwRequestFactory,
                                          @Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor) {
        this.mppwRequestFactory = mppwRequestFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<Void> execute(TransferDataRequest dataRequest) {
        return Future.future(promise -> {
            AdbKafkaMppwTransferRequest transferRequest = mppwRequestFactory.create(dataRequest);
            adbQueryExecutor.executeInTransaction(transferRequest.getFirstTransaction())
                    .compose(v -> adbQueryExecutor.executeInTransaction(transferRequest.getSecondTransaction()))
                    .onComplete(promise);
        });
    }
}
