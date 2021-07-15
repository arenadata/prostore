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
package io.arenadata.dtm.query.execution.core.configuration;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DtmKafkaContainer extends GenericContainer<DtmKafkaContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-kafka");
    private static final String DEFAULT_TAG = "5.4.3";
    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    public static final int KAFKA_PORT = 9093;
    private String kafkaHost = "localhost";
    public static final int ZOOKEEPER_PORT = 2181;
    private static final int PORT_NOT_ASSIGNED = -1;
    protected String externalZookeeperConnect;
    private int port;

    public DtmKafkaContainer(DockerImageName dockerImageName, String kafkaHost) {
        super(dockerImageName);
        this.externalZookeeperConnect = null;
        this.port = -1;
        this.kafkaHost = kafkaHost;
        dockerImageName.assertCompatibleWith(new DockerImageName[]{DEFAULT_IMAGE_NAME});
        this.withExposedPorts(new Integer[]{9093});
        this.withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092");
        this.withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        this.withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");
        this.withEnv("KAFKA_BROKER_ID", "1");
        this.withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        this.withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
        this.withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", "9223372036854775807");
        this.withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
    }

    public DtmKafkaContainer withEmbeddedZookeeper() {
        this.externalZookeeperConnect = null;
        return (DtmKafkaContainer) this.self();
    }

    public DtmKafkaContainer withExternalZookeeper(String connectString) {
        this.externalZookeeperConnect = connectString;
        return (DtmKafkaContainer) this.self();
    }

    public String getBootstrapServers() {
        if (this.port == -1) {
            throw new IllegalStateException("You should start Kafka container first");
        } else {
            return String.format("PLAINTEXT://%s:%s", "dtm.kafka.it.broker", KAFKA_PORT);
        }
    }

    protected void doStart() {
        this.withCommand(new String[]{"sh", "-c", "while [ ! -f /testcontainers_start.sh ]; do sleep 0.1; done; /testcontainers_start.sh"});
        if (this.externalZookeeperConnect == null) {
            this.addExposedPort(2181);
        }

        super.doStart();
    }

    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        try {
            super.containerIsStarting(containerInfo, reused);
            this.port = this.getMappedPort(9093);
            if (!reused) {
                String command = "#!/bin/bash\n";
                String zookeeperConnect;
                if (this.externalZookeeperConnect != null) {
                    zookeeperConnect = this.externalZookeeperConnect;
                } else {
                    zookeeperConnect = "localhost:2181";
                    command = command + "echo 'clientPort=2181' > zookeeper.properties\n";
                    command = command + "echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties\n";
                    command = command + "echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties\n";
                    command = command + "zookeeper-server-start zookeeper.properties &\n";
                }

                command = command + "export KAFKA_ZOOKEEPER_CONNECT='" + zookeeperConnect + "'\n";
                command = command + "export KAFKA_ADVERTISED_LISTENERS='" + (String) Stream.concat(Stream.of(this.getBootstrapServers()), containerInfo.getNetworkSettings().getNetworks().values().stream().map((it) -> {
                    return "BROKER://" + it.getIpAddress() + ":9092";
                })).collect(Collectors.joining(",")) + "'\n";
                command = command + ". /etc/confluent/docker/bash-config \n";
                command = command + "/etc/confluent/docker/configure \n";
                command = command + "/etc/confluent/docker/launch \n";
                this.copyFileToContainer(Transferable.of(command.getBytes(StandardCharsets.UTF_8), 511), "/testcontainers_start.sh");
            }
        } catch (Throwable var5) {
            throw var5;
        }
    }
}
