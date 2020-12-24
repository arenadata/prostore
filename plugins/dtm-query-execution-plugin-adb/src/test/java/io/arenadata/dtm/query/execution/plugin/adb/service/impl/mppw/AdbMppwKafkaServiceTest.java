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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdbMppwKafkaServiceTest {

    private MppwKafkaLoadRequest kafkaLoadRequest;
    private MppwTransferDataRequest mppwTransferDataRequest;
    private MppwKafkaRequestContext kafkaRequestContext;

    @Test
    void convertKafkaRequestContextTest() {
        kafkaLoadRequest = MppwKafkaLoadRequest.builder()
        .requestId(UUID.randomUUID().toString())
                .datamart("test")
                .tableName("tab_")
                .schema(new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"accounts_ext_dtm_536\",\"namespace\":\"dtm_536\",\"fields\":[{\"name\":\"account_id\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"account_type\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}"))
        .build();

        mppwTransferDataRequest = MppwTransferDataRequest.builder()
                .columnList(Arrays.asList("account_id", "account_type"))
                .keyColumnList(Arrays.asList("account_id"))
                .hotDelta(3L)
                .tableName("tab_")
                .datamart("test")
                .build();

        kafkaRequestContext = new MppwKafkaRequestContext(kafkaLoadRequest, mppwTransferDataRequest);

        final String encodeRequest = Json.encode(kafkaRequestContext);

        final MppwKafkaLoadRequest loadRequestDecoded =
                Json.decodeValue(((JsonObject) Json.decodeValue(encodeRequest))
                        .getJsonObject("mppwKafkaLoadRequest").toString(), MppwKafkaLoadRequest.class);
        final MppwTransferDataRequest transferDataRequestDecoded =
                Json.decodeValue(((JsonObject) Json.decodeValue(encodeRequest))
                        .getJsonObject("mppwTransferDataRequest").toString(), MppwTransferDataRequest.class);
        assertEquals(kafkaLoadRequest, loadRequestDecoded);
        assertEquals(mppwTransferDataRequest, transferDataRequestDecoded);
    }
}
