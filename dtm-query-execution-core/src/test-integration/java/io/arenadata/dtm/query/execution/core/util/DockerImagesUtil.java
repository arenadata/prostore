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
package io.arenadata.dtm.query.execution.core.util;

public class DockerImagesUtil {

    public static final String KAFKA = "confluentinc/cp-kafka";
    public static final String ZOOKEEPER = "confluentinc/cp-zookeeper:4.0.0";
    public static final String DTM_KAFKA_EMULATOR_READER = "ci.arenadata.io/connector-kafka-emulator-reader:latest";
    public static final String DTM_KAFKA_EMULATOR_WRITER = "ci.arenadata.io/db-writer:latest";
    public static final String DTM_KAFKA_STATUS_MONITOR = "ci.arenadata.io/dtm-status-monitor:latest";
    public static final String DTM_VENDOR_EMULATOR = "ci.arenadata.io/dtm-vendor-emulator:latest";
    public static final String DTM_CORE = "ci.arenadata.io/dtm-core:latest";
    public static final String ADQM = "yandex/clickhouse-server:latest";
    public static final String ADB = "pivotaldata/gpdb-devel";
    public static final String MARIA_DB = "mariadb:10.5.3";

}
