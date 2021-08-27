# Prostore
Prostore is an open-source integration system providing a temporal DB unified interface to the heterogenous data store. It’s purposed for a datamart implementation.

## Useful links
[Documentation (Rus)](https://arenadata.github.io/docs_prostore/)

## Local deployment

### App configuration
All app configuration files are placed into dtm-query-execution-core/config folder.
We use Spring Boot profiles to separate settings for the different environments.
Actually we supports 3 environments:
* dev - all connections are pointed to the Yandex.Cloud
* local - Service db, Kafka and Zookeeper are local, ADB, ADG and ADQM are similar to dev profile.
* local-debug - Service db, Kafka, Zookeeper, ADB, ADG and ADQM are local. See [Local debug](#local-debug).

Default profile actually similar to the dev profile.

We can specify profile via environment variable SPRING_PROFILES_ACTIVE or via specifing argument to java -Dspring.profiles.active=

For MPPW support you also need to confugure & run [dtm-vendor-emulator](https://github.com/arenadata/dtm-vendor-emulator) & [dtm-adb-emulator-writer](https://github.com/arenadata/dtm-adb-emulator-writer).

### Build application

#### Start required services

```shell script
cd dtm-query-execution-core
docker-compose -f environment/docker-compose-build.yml up -d
```

#### Build project using maven

```shell script
# without any tests
mvn package -P local -D skipTests

# with unit and integration tests
mvn verify -P local
```

### Run application
#### Run main service as a single jar

```shell script
cd dtm-query-execution-core
java -Dspring.profiles.active=dev -jar target/dtm-query-execution-core-5.1.0.jar
```
and use port 35286 for debugger and 8088 fo DTM JDBC driver.

## Setup IDE

Use profile `local` for project builder.

Setup run configuration for core application:
1. Working dir - `dtm-query-execution-core`.
2. Main class - `io.arenadata.dtm.query.execution.core.ServiceQueryExecutionApplication`.
3. VM options - `-Dspring.profiles.active=dev`.

## Setup JDBC test client

Use [DTM JDBC driver](dtm-jdbc-driver/README.md).
URL is `jdbc:adtm://<host>:<port>/`:
- `host` is host of dtm-query-execution-core (`localhost`)
- `port` is port of dtm-query-execution-core (see active `application.yml` for dtm-query-execution-core)
