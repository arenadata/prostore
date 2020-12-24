# Плагин для ADG (dtm-query-execution-plugin-adg)

### Сборка

`mvn clean package`

### Публикация в локальный репозиторий

`mvn install`

### Публикация в удаленный репозиторий

`mvn deploy`

### Запуск интеграционных тестов (IT)

`mvn verify -DskipITs=false`

Настройки
---
|    | Название                | Значение                | Описание                  |
|----|-------------------------|-------------------------|---------------------------|
| 1  | KAFKA_BOOTSTRAP_SERVERS | kafka-1.dtm.local:9092  | Подключение к Kafka       |
| 2  | ZOOKEEPER_HOSTS         | kafka-1.dtm.local:9092  | Подключение к Zookeeper   |
| 3  | TARANTOOL_DB_HOST       | 10.18.84.10             | Адрес сервера TT          |
| 4  | TARANTOOL_DB_PORT       | 3311                    | Порт сервера TT           |
| 5  | TARANTOOL_DB_USER       | admin                   | Имя пользователя TT       |
| 6  | TARANTOOL_DB_PASS       | 123                     | Пароль пользователя TT    |
| 7  | TARANTOOL_CATRIDGE_URL  | http://10.18.84.10:8811 | Адрес Tatantool Cartridge  |
| 8  | SCHEMA_REGISTRY_URL     | http://10.18.84.6:8081  | Адрес Schema Registry     |


