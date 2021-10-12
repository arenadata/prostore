# Prostore
Prostore is an open-source integration system providing a temporal DB unified interface to the heterogenous data store. It’s purposed for a datamart implementation.

## Useful links
[Documentation (Rus)](https://arenadata.github.io/docs_prostore/)

## Local deployment
The main prerequisites include git, Java and Apache Maven.

### The cloning and building of the Prostore repository
```shell script
# clone
git clone https://github.com/arenadata/prostore ~/
# build without any tests
cd ~/prostore
mvn clean install -DskipTests=true
```
The resulting jar file is located in the `dtm-query-execution-core/target` folder.

### App configuration
The Prostore configuration file is located in the `dtm-query-execution-core/config` folder.
The Prostore application looks for the configuration in the same subfolder (target) where `dtm-query-execution-core-<version>.jar` is executed.
So we create the respective symbolic link
```shell script
sudo ln -s ~/prostore/dtm-query-execution-core/config/application.yml ~/prostore/dtm-query-execution-core/target/application.yml
```
If no configuration file is located, then the Prostore application uses its internal default configuration.

### Run application

#### Run prerequisite obligatory supporting services
-    Zookeeper,
-    Kafka,
-    set of respective DBMS,
-    kafka-DBMS connectors (e.g. see [kafka-postgres-connector](https://github.com/arenadata/kafka-postgres-connector)),
-    [Prostore status monitor](https://github.com/arenadata/prostore/tree/master/dtm-status-monitor).

#### Run main service as a single jar with Spring Boot starting on the default port 8080
```shell script
cd ~/prostore/dtm-query-execution-core/target
java -jar dtm-query-execution-core-<version>.jar
```

#### Change default port for Spring Boot
-    change the value of the key `management:server:port` in the configuration file,
-    run the main service `java -Dserver.port=<DTM_METRICS_PORT> -jar dtm-query-execution-core-<version>.jar`.

## Setup JDBC test client

Use [DTM JDBC driver](dtm-jdbc-driver/README.md).
URL is `jdbc:adtm://<host>:<port>/`:
- `host` is host of dtm-query-execution-core (`localhost`)
- `port` is port of dtm-query-execution-core (see actual `application.yml` for dtm-query-execution-core)

See also [connection with JDBC-client (Rus)](https://arenadata.github.io/docs_prostore/working_with_system/connection/connection_via_sql_client/connection_via_sql_client.html)
