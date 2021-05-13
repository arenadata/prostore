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
package io.arenadata.dtm.query.execution.core.delta.repository.executor;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaException;
import io.arenadata.dtm.query.execution.core.delta.dto.Delta;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.Data;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public abstract class DeltaServiceDaoExecutorHelper {
    public static final String SEQUENCE_NUMBER_TEMPLATE = "0000000000";
    protected static final byte[] EMPTY_DATA = null;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    protected final ZookeeperExecutor executor;
    protected final String envPath;

    protected DeltaServiceDaoExecutorHelper(ZookeeperExecutor executor, String envName) {
        this.executor = executor;
        this.envPath = "/" + envName;
    }

    protected Op createDatamartNodeOp(String datamartPath, String nodeName) {
        return Op.create(datamartPath + nodeName, EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    protected String getDeltaPath(String datamart) {
        return String.format("%s/%s/delta", envPath, datamart);
    }

    protected String getDatamartPath(String datamart) {
        return String.format("%s/%s", envPath, datamart);
    }

    protected String getDeltaDatePath(String datamart, LocalDate localDate) {
        return getDeltaPath(datamart) + "/date/" + localDate.toString();
    }

    protected String getDeltaDateTimePath(String datamart, LocalDateTime deltaDate) {
        return String.format("%s/%s", getDeltaDatePath(datamart, deltaDate.toLocalDate()), TIME_FORMATTER.format(deltaDate.toLocalTime()));
    }

    protected String getDeltaNumPath(String datamart, Long deltaNum) {
        return getDeltaPath(datamart) + "/num/" + deltaNum;
    }

    protected String getWriteOpPath(String datamart, Long opNum) {
        return getDatamartPath(datamart) + "/run/" + toSequenceNumber(opNum);
    }

    protected byte[] serializedDelta(Delta delta) {
        try {
            return DatabindCodec.mapper().writeValueAsBytes(delta);
        } catch (JsonProcessingException e) {
            throw new DeltaException(String.format("Can't serialize delta [%s]", delta), e);
        }
    }

    protected byte[] serializedOkDelta(OkDelta delta) {
        try {
            return DatabindCodec.mapper().writeValueAsBytes(delta);
        } catch (JsonProcessingException e) {
            throw new DeltaException(String.format("Can't serialize delta ok [%s]", delta), e);
        }
    }

    protected Delta deserializedDelta(byte[] bytes) {
        try {
            return DatabindCodec.mapper().readValue(bytes, Delta.class);
        } catch (IOException e) {
            throw new DeltaException("Can't deserialize Delta", e);
        }
    }

    protected OkDelta deserializedOkDelta(byte[] bytes) {
        try {
            return DatabindCodec.mapper().readValue(bytes, OkDelta.class);
        } catch (IOException e) {
            throw new DeltaException("Can't deserialize Delta Ok", e);
        }
    }

    protected byte[] serializeDeltaWriteOp(DeltaWriteOp deltaWriteOp) {
        try {
            return DatabindCodec.mapper().writeValueAsBytes(deltaWriteOp);
        } catch (JsonProcessingException e) {
            throw new DeltaException("Can't serialize deltaWriteOp", e);
        }
    }

    protected DeltaWriteOp deserializeDeltaWriteOp(byte[] writeOpData) {
        try {
            return DatabindCodec.mapper().readValue(writeOpData, DeltaWriteOp.class);
        } catch (IOException e) {
            throw new DeltaException("Can't deserialize deltaWriteOp", e);
        }
    }

    protected String toSequenceNumber(long number) {
        return SEQUENCE_NUMBER_TEMPLATE.substring(String.valueOf(number).length()) + number;
    }

    @Data
    protected static final class DeltaContext {
        private Delta delta;
    }
}
