# DTM core services & plugins
Main project of data mart core.

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
java -Dspring.profiles.active=dev -jar target/dtm-query-execution-core-4.0.jar
```

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
