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
package io.arenadata.dtm.query.execution.core;

import io.arenadata.dtm.query.execution.core.configuration.DtmKafkaContainer;
import io.arenadata.dtm.query.execution.core.configuration.IntegrationTestConfiguration;
import io.arenadata.dtm.query.execution.core.factory.PropertyFactory;
import io.arenadata.dtm.query.execution.core.generator.VendorEmulatorServiceImpl;
import io.arenadata.dtm.query.execution.core.kafka.StatusMonitorServiceImpl;
import io.arenadata.dtm.query.execution.core.query.client.SqlClientFactoryImpl;
import io.arenadata.dtm.query.execution.core.query.client.SqlClientProviderImpl;
import io.arenadata.dtm.query.execution.core.query.executor.QueryExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.util.DockerImagesUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.PropertySource;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

@Testcontainers
@ActiveProfiles("it_test")
@SpringBootTest(classes = {IntegrationTestConfiguration.class,
        ZookeeperExecutor.class,
        ZookeeperConnectionProvider.class,
        StatusMonitorServiceImpl.class,
        QueryExecutorImpl.class,
        SqlClientProviderImpl.class,
        SqlClientFactoryImpl.class,
        VendorEmulatorServiceImpl.class})
@Slf4j
public abstract class AbstractCoreDtmIT {

    public static final int ZK_PORT = 2181;
    private static final int CLICKHOUSE_PORT = 8123;
    public static final PropertySource<?> dtmProperties;
    public static final GenericContainer<?> zkDsContainer;
    public static final GenericContainer<?> zkKafkaContainer;
    public static final DtmKafkaContainer kafkaContainer;
    public static final GenericContainer<?> mariaDBContainer;
    public static final GenericContainer<?> adqmContainer;
    public static final GenericContainer<?> dtmCoreContainer;
    public static final GenericContainer<?> dtmKafkaReaderContainer;
    public static final GenericContainer<?> dtmKafkaWriterContainer;
    public static final GenericContainer<?> dtmVendorEmulatorContainer;
    public static final GenericContainer<?> dtmKafkaStatusMonitorContainer;
    private static final Network network = Network.SHARED;
    private static final Map<GenericContainer<?>, ContainerInfo> containerMap = new HashMap<>();

    static {
        dtmProperties = PropertyFactory.createPropertySource("application-it_test.yml");
        zkDsContainer = createZkDsContainer();
        zkKafkaContainer = createZkKafkaContainer();
        kafkaContainer = createKafkaContainer();
        adqmContainer = createAdqmContainer();
        dtmKafkaReaderContainer = createKafkaReaderContainer();
        dtmKafkaWriterContainer = createKafkaWriterContainer();
        mariaDBContainer = createMariaDbContainer();
        dtmVendorEmulatorContainer = createVendorEmulatorContainer();
        dtmKafkaStatusMonitorContainer = createKafkaStatusMonitorContainer();
        dtmCoreContainer = createDtmCoreContainer();

        Stream.of(
                zkDsContainer,
                zkKafkaContainer,
                kafkaContainer,
                mariaDBContainer
        ).parallel().forEach(GenericContainer::start);
        Stream.of(
                adqmContainer,
                dtmKafkaReaderContainer,
                dtmKafkaWriterContainer,
                dtmKafkaStatusMonitorContainer,
                dtmVendorEmulatorContainer,
                dtmCoreContainer
        ).forEach(GenericContainer::start);
        initContainerMap();
        containerMap.forEach((key, value) -> log.info("Started container for integration tests: {}, host: {}, port: {}, image: {}",
                value.getName(), key.getHost(), value.getPort(), key.getDockerImageName()));
    }

    private static GenericContainer<?> createZkDsContainer() {
        return new GenericContainer<>(DockerImagesUtil.ZOOKEEPER)
                .withNetwork(network)
                .withExposedPorts(ZK_PORT)
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("core.datasource.zookeeper.connection-string")).toString())
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZK_PORT));
    }

    private static GenericContainer<?> createZkKafkaContainer() {
        return new GenericContainer<>(DockerImagesUtil.ZOOKEEPER)
                .withNetwork(network)
                .withExposedPorts(ZK_PORT)
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("core.kafka.cluster.zookeeper.connection-string")).toString())
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZK_PORT));
    }

    private static DtmKafkaContainer createKafkaContainer() {
        final String kafkaHost = Objects.requireNonNull(
                dtmProperties.getProperty("statusMonitor.brokersList")).toString().split(":")[0];
        return new DtmKafkaContainer(DockerImageName.parse(DockerImagesUtil.KAFKA),
                kafkaHost)
                .withNetwork(network)
                .withNetworkAliases(kafkaHost)
                .withExternalZookeeper(Objects.requireNonNull(
                        dtmProperties.getProperty("core.kafka.cluster.zookeeper.connection-string")).toString()
                        + ":" + ZK_PORT);
    }

    private static GenericContainer createAdqmContainer() {
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.ADQM))
                .withNetwork(network)
                .withExposedPorts(Integer.valueOf(Objects.requireNonNull(
                        dtmProperties.getProperty("adqm.datasource.hosts")).toString().split(":")[1]))
                .withReuse(false)
                .withCopyFileToContainer(MountableFile.forClasspathResource("config/adqm/config.xml"),
                        "/etc/clickhouse-server/config.xml")
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("adqm.datasource.hosts")).toString().split(":")[0]);
    }

    private static GenericContainer<?> createKafkaStatusMonitorContainer() {
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.DTM_KAFKA_STATUS_MONITOR))
                .withNetwork(network)
                .withExposedPorts((Integer) Objects.requireNonNull(
                        dtmProperties.getProperty("statusMonitor.port")))
                .withEnv("STATUS_MONITOR_CONSUMERS", Objects.requireNonNull(
                        dtmProperties.getProperty("statusMonitor.consumersCount")).toString())
                .withEnv("STATUS_MONITOR_BROKERS", Objects.requireNonNull(
                        dtmProperties.getProperty("statusMonitor.brokersList")).toString())
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("statusMonitor.host")).toString());
    }

    private static GenericContainer<?> createMariaDbContainer() {
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.MARIA_DB))
                .withNetwork(network)
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.host"))
                        .toString())
                .withExposedPorts(Integer.valueOf(Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.port"))
                        .toString()))
                .withEnv("MYSQL_DATABASE", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.name"))
                        .toString())
                .withEnv("MYSQL_USER", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.user"))
                        .toString())
                .withEnv("MYSQL_PASSWORD", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.password"))
                        .toString())
                .withEnv("MYSQL_ROOT_PASSWORD", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.rootPass"))
                        .toString())
                .withEnv("TZ", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.timeZone"))
                        .toString());
    }

    private static GenericContainer<?> createVendorEmulatorContainer() {
        ToStringConsumer toStringConsumer = new ToStringConsumer();
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.DTM_VENDOR_EMULATOR))
                .withLogConsumer(toStringConsumer)
                .withNetwork(network)
                .withExposedPorts((Integer) Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.port")))
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.host"))
                        .toString())
                .withEnv("KAFKA_BROKERS", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.kafkaBrokers"))
                        .toString())
                .withEnv("KAFKA_PARTITION_SIZE", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.kafkaPartitionSize"))
                        .toString())
                .withEnv("MARIA_DB_HOST", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.host"))
                        .toString())
                .withEnv("MARIA_DB_PORT", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.port"))
                        .toString())
                .withEnv("MARIA_DB_NAME", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.name"))
                        .toString())
                .withEnv("MARIA_DB_USERNAME", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.user"))
                        .toString())
                .withEnv("MARIA_DB_PASS", Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.password"))
                        .toString());
    }

    private static GenericContainer<?> createKafkaWriterContainer() {
        ToStringConsumer toStringConsumer = new ToStringConsumer();
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.DTM_KAFKA_EMULATOR_WRITER))
                .withLogConsumer(toStringConsumer)
                .withNetwork(network)
                .withExposedPorts((Integer) Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.port")))
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.host")).toString())
                .withEnv("ADQM_DB_NAME", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.adqm.dbName"))
                        .toString())
                .withEnv("ADQM_USERNAME", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.adqm.user"))
                        .toString())
                .withEnv("ADQM_HOSTS", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.adqm.hosts"))
                        .toString())
                .withEnv("ENV", Objects.requireNonNull(dtmProperties.getProperty("core.env.name"))
                        .toString())
                .withEnv("KAFKA_BOOTSTRAP_SERVERS", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.kafkaBrokers"))
                        .toString())
                .withEnv("KAFKA_CONSUMER_GROUP_ID", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.consumerGroupId"))
                        .toString())
                .withEnv("DATA_WORKER_POOL_SIZE", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.dataWorkerPoolSize"))
                        .toString())
                .withEnv("TASK_WORKER_POOL_SIZE", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.taskWorkerPoolSize"))
                        .toString())
                .withEnv("ADB_DB_NAME", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.adb.dbName"))
                        .toString())
                .withEnv("ADB_USERNAME", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.adb.user"))
                        .toString())
                .withEnv("ADB_HOST", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.adb.host"))
                        .toString())
                .withEnv("ADB_PORT", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.adb.port"))
                        .toString());
    }

    private static GenericContainer<?> createKafkaReaderContainer() {
        ToStringConsumer toStringConsumer = new ToStringConsumer();
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.DTM_KAFKA_EMULATOR_READER))
                .withLogConsumer(toStringConsumer)
                .withNetwork(network)
                .withExposedPorts((Integer) Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorReader.port")))
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorReader.host")).toString())
                .withEnv("ADQM_DB_NAME", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorReader.adqm.dbName"))
                        .toString())
                .withEnv("ADQM_USERNAME", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorReader.adqm.user"))
                        .toString())
                .withEnv("ADQM_HOSTS", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorReader.adqm.hosts"))
                        .toString())
                .withEnv("ADB_USERNAME", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorReader.adb.user"))
                        .toString())
                .withEnv("ADB_HOST", Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorReader.adb.host"))
                        .toString());
    }

    private static GenericContainer<?> createDtmCoreContainer() {
        ToStringConsumer toStringConsumer = new ToStringConsumer();
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.DTM_CORE))
                .withLogConsumer(toStringConsumer)
                .withExposedPorts((Integer) dtmProperties.getProperty("core.http.port"),
                        (Integer) dtmProperties.getProperty("management.server.port"))
                .withNetworkAliases("test-it-dtm-core")
                .withNetwork(network)
                .withEnv("DTM_CORE_PORT", Objects.requireNonNull(dtmProperties.getProperty("core.http.port")).toString())
                .withEnv("DTM_METRICS_PORT", Objects.requireNonNull(dtmProperties.getProperty("management.server.port")).toString())
                .withEnv("DTM_NAME", Objects.requireNonNull(dtmProperties.getProperty("core.env.name")).toString())
                .withEnv("CORE_PLUGINS_ACTIVE", Objects.requireNonNull(dtmProperties.getProperty("core.plugins.active")).toString())
                .withEnv("EDML_DATASOURCE", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.sourceType")).toString())
                .withEnv("EDML_DEFAULT_CHUNK_SIZE", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.defaultChunkSize")).toString())
                .withEnv("EDML_STATUS_CHECK_PERIOD_MS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.pluginStatusCheckPeriodMs")).toString())
                .withEnv("EDML_FIRST_OFFSET_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.firstOffsetTimeoutMs")).toString())
                .withEnv("EDML_CHANGE_OFFSET_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.changeOffsetTimeoutMs")).toString())
                .withEnv("ZOOKEEPER_DS_ADDRESS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.zookeeper.connection-string")).toString())
                .withEnv("ZOOKEEPER_DS_CONNECTION_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.zookeeper.connection-timeout-ms")).toString())
                .withEnv("ZOOKEEPER_KAFKA_ADDRESS", Objects.requireNonNull(dtmProperties.getProperty("core.kafka.cluster.zookeeper.connection-string")).toString())
                .withEnv("ZOOKEEPER_KAFKA_CONNECTION_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.kafka.cluster.zookeeper.connection-timeout-ms")).toString())
                .withEnv("ZOOKEEPER_KAFKA_SESSION_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.kafka.cluster.zookeeper.session-timeout-ms")).toString())
                .withEnv("KAFKA_INPUT_STREAM_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.kafka.admin.inputStreamTimeoutMs")).toString())
                .withEnv("STATUS_MONITOR_URL", Objects.requireNonNull(dtmProperties.getProperty("core.kafka.statusMonitorUrl")).toString())
                .withEnv("ADB_DB_NAME", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.database")).toString())
                .withEnv("ADB_USERNAME", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.user")).toString())
                .withEnv("ADB_PASS", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.password")).toString())
                .withEnv("ADB_HOST", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.host")).toString())
                .withEnv("ADB_PORT", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.port")).toString())
                .withEnv("ADB_MPPW_POOL_SIZE", Objects.requireNonNull(dtmProperties.getProperty("adb.mppw.poolSize")).toString())
                .withEnv("ADB_LOAD_GROUP", Objects.requireNonNull(dtmProperties.getProperty("adb.mppw.consumerGroup")).toString())
                .withEnv("ADB_MPPW_STOP_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("adb.mppw.stopTimeoutMs")).toString())
                .withEnv("TARANTOOL_DB_HOST", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.host")).toString())
                .withEnv("TARANTOOL_DB_PORT", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.port")).toString())
                .withEnv("TARANTOOL_DB_USER", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.user")).toString())
                .withEnv("TARANTOOL_DB_PASS", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.password")).toString())
                .withEnv("TARANTOOL_DB_OPER_TIMEOUT", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.operationTimeout")).toString())
                .withEnv("TARANTOOL_DB_RETRY_COUNT", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.retryCount")).toString())
                .withEnv("TARANTOOL_CATRIDGE_URL", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.cartridge.url")).toString())
                .withEnv("ADQM_DB_NAME", Objects.requireNonNull(dtmProperties.getProperty("adqm.datasource.database")).toString())
                .withEnv("ADQM_USERNAME", Objects.requireNonNull(dtmProperties.getProperty("adqm.datasource.user")).toString())
                .withEnv("ADQM_HOSTS", Objects.requireNonNull(dtmProperties.getProperty("adqm.datasource.hosts")).toString())
                .withEnv("ADQM_CLUSTER", Objects.requireNonNull(dtmProperties.getProperty("adqm.ddl.cluster")).toString())
                .withEnv("ADQM_TTL_SEC", Objects.requireNonNull(dtmProperties.getProperty("adqm.ddl.ttlSec")).toString())
                .withEnv("ADQM_ARCHIVE_DISK", Objects.requireNonNull(dtmProperties.getProperty("adqm.ddl.archiveDisk")).toString())
                .withEnv("ADQM_CONSUMER_GROUP", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.consumerGroup")).toString())
                .withEnv("ADQM_BROKERS", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.kafkaBrokers")).toString())
                .withEnv("ADQM_MPPR_CONNECTOR_HOST", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppr.host")).toString())
                .withEnv("ADQM_MPPR_CONNECTOR_PORT", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppr.port")).toString())
                .withEnv("ADQM_MPPR_CONNECTOR_URL", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppr.url")).toString())
                .withEnv("ADQM_MPPW_LOAD_TYPE", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.loadType")).toString())
                .withEnv("ADQM_REST_START_LOAD_URL", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.restStartLoadUrl")).toString())
                .withEnv("ADQM_REST_STOP_LOAD_URL", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.restStopLoadUrl")).toString())
                .withEnv("ADQM_REST_LOAD_GROUP", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.restLoadConsumerGroup")).toString());
    }

    private static void initContainerMap() {
        containerMap.put(zkDsContainer, new ContainerInfo("Zookeeper service db",
                zkDsContainer.getMappedPort(ZK_PORT)));
        containerMap.put(zkKafkaContainer, new ContainerInfo("Zookeeper kafka",
                zkKafkaContainer.getMappedPort(ZK_PORT)));
        containerMap.put(kafkaContainer, new ContainerInfo("Kafka",
                kafkaContainer.getMappedPort(Integer.parseInt(Objects.requireNonNull(
                        dtmProperties.getProperty("statusMonitor.brokersList")).toString().split(":")[1]))));
        containerMap.put(adqmContainer, new ContainerInfo("Adqm",
                adqmContainer.getMappedPort(CLICKHOUSE_PORT)));
        containerMap.put(dtmKafkaReaderContainer, new ContainerInfo("Dtm kafka emulator reader",
                dtmKafkaReaderContainer.getMappedPort(Integer.parseInt(Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorReader.port"))
                        .toString()))));
        containerMap.put(dtmKafkaWriterContainer, new ContainerInfo("Dtm kafka emulator writer",
                dtmKafkaWriterContainer.getMappedPort(Integer.parseInt(Objects.requireNonNull(
                        dtmProperties.getProperty("kafkaEmulatorWriter.port"))
                        .toString()))));
        containerMap.put(mariaDBContainer, new ContainerInfo("Vendor emulator mariaDb",
                mariaDBContainer.getMappedPort(Integer.parseInt(Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.mariadb.port"))
                        .toString()))));
        containerMap.put(dtmVendorEmulatorContainer, new ContainerInfo("Dtm vendor emulator",
                dtmVendorEmulatorContainer.getMappedPort(Integer.parseInt(Objects.requireNonNull(
                        dtmProperties.getProperty("vendorEmulator.port"))
                        .toString()))));
        containerMap.put(dtmKafkaStatusMonitorContainer, new ContainerInfo("Dtm kafka status monitor",
                dtmKafkaStatusMonitorContainer.getMappedPort(Integer.parseInt(Objects.requireNonNull(
                        dtmProperties.getProperty("statusMonitor.port"))
                        .toString()))));
        containerMap.put(dtmCoreContainer, new ContainerInfo("Dtm core",
                dtmCoreContainer.getMappedPort(Integer.parseInt(Objects.requireNonNull(
                        dtmProperties.getProperty("core.http.port"))
                        .toString()))));
    }

    public static String getZkDsConnectionStringAsExternal() {
        return zkDsContainer.getHost() + ":" + zkDsContainer.getMappedPort(ZK_PORT);
    }

    public static String getZkKafkaConnectionString() {
        return Objects.requireNonNull(dtmProperties.getProperty("core.kafka.cluster.zookeeper.connection-string")).toString()
                + ":" + ZK_PORT;
    }

    public static String getKafkaStatusMonitorHostExternal() {
        return dtmKafkaStatusMonitorContainer.getHost();
    }

    public static int getKafkaStatusMonitorPortExternal() {
        return dtmKafkaStatusMonitorContainer.getMappedPort((Integer) Objects.requireNonNull(
                dtmProperties.getProperty("statusMonitor.port")));
    }

    public static String getDtmCoreHostExternal() {
        return dtmCoreContainer.getHost();
    }

    public static int getDtmCorePortExternal() {
        return dtmCoreContainer.getMappedPort(
                Integer.parseInt(Objects.requireNonNull(dtmProperties.getProperty("core.http.port")).toString()));
    }

    public static int getDtmMetricsPortExternal() {
        return dtmCoreContainer.getMappedPort((Integer) Objects.requireNonNull(dtmProperties.getProperty("management.server.port")));
    }

    public static String getVendorEmulatorHostExternal() {
        return dtmVendorEmulatorContainer.getHost();
    }

    public static int getVendorEmulatorPortExternal() {
        return dtmVendorEmulatorContainer.getMappedPort((Integer) Objects.requireNonNull(
                dtmProperties.getProperty("vendorEmulator.port")));
    }

    public static String getDtmCoreHostPortExternal() {
        return getDtmCoreHostExternal() + ":" + getDtmCorePortExternal();
    }

    public String getAdqmConsumerGroup() {
        return Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.restLoadConsumerGroup")).toString();
    }

    public static String getJdbcDtmConnectionString() {
        return "jdbc:adtm://" + getDtmCoreHostPortExternal() + "/";
    }

    public static String getEntitiesPath(String datamartMnemonic) {
        return String.format("/%s/%s/entity",
                Objects.requireNonNull(dtmProperties.getProperty("core.env.name")).toString(),
                datamartMnemonic);
    }

    public static String getEntityPath(String datamartMnemonic, String entityName) {
        return String.format("/%s/%s/entity/%s",
                Objects.requireNonNull(dtmProperties.getProperty("core.env.name")).toString(),
                datamartMnemonic,
                entityName);
    }

    public static String getDeltaPath(String datamartMnemonic) {
        return String.format("/%s/%s/delta",
                Objects.requireNonNull(dtmProperties.getProperty("core.env.name")).toString(),
                datamartMnemonic);
    }

    @Data
    @AllArgsConstructor
    private static class ContainerInfo {
        private int port;
        private String name;

        public ContainerInfo(String name, Integer mappedPort) {
            this.name = name;
            this.port = mappedPort;
        }
    }
}
