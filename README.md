# DTM core services & plugins
Datamart main project.

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

#### Load initial data

To load schema changes use sub-project [dtm-migration](dtm-migration/README.md) and run:
```shell script
cd dtm-migration
mvn spring-boot:run
```

#### Build project using maven

```shell script
# without any tests
mvn package -P local -D skipTests

# with unit and integration tests
mvn verify -P local
```

### Run application

!!! Please note what actually we hardcode jar version while running or building docker image, so please double-check it while running application

#### Run main service as a single jar

```shell script
cd dtm-query-execution-core
java -Dspring.profiles.active=dev -jar target/dtm-query-execution-core-2.2.1-SNAPSHOT.jar
```

#### Run main service as a docker container

```shell script
cd dtm-query-execution-core
docker-compose -f environment/docker-compose-dev.yml up -d
docker logs -f dtm-query-execution-core
```

#### Run main service and related services (ServiceDB, Kafka, Zookeeper) 

```shell script
cd dtm-query-execution-core
docker-compose -f environment/docker-compose-local.yml up -d
docker logs -f dtm-query-execution-core
```
After that service will listen on the 9090 port

It runs:
* Zookeeper
* Kafka
* dtm-query-execution-core
* ADB
* ADG
* ADQM

Add `127.0.0.1	kafka-1.dtm.local` to `/etc/hosts`. It is required for tests and local debug.

## Local debug

##### Run local environment:
```shell script
docker-compose -f dtm-query-execution-core/environment/docker-compose-local-debug.yml up -d
cd dtm-migration
mvn spring-boot:run
docker run -d --rm -p 15432:6000 --name gpdb-pxf-cluster ci.arenadata.io/gpdb-pxf:20200626
docker exec -it gpdb-pxf-cluster bash
```
##### Inside opened `gpdb-pxf-cluster` console:
```shell script
/initialize_cluster
sudo su - gpadmin
echo "host all gpadmin 0.0.0.0/0 trust" >> $MASTER_DATA_DIRECTORY/pg_hba.conf
gpstop -au
```
##### Run core with `local-debug` profile inside IDE or:
```shell script
cd dtm-query-execution-core
java -agentlib:jdwp=transport=dt_socket,address=35286,server=y,suspend=n -Dspring.profiles.active=local-debug -jar target/dtm-query-execution-core-<version>.jar
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
