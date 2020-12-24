--
-- Copyright © 2020 ProStore
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

create database demo;
use demo;
-- 1. Запустить 100 операций
-- 2. Продемонстрировать созданные таблицы в хранилище (ADB, ADG, ADQM)
-- 3. Продемонстрируем мета-данные в Zookeeper http://10.92.3.3:10001/node?path=/adtm/orig/demo/entity
-- 4. Убедимся в корректном отображении схемы данных в системных представлениях
select * from INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME = 'DEMO';
select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'DEMO';
select * from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = 'DEMO';
select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_SCHEMA = 'DEMO';
select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where TABLE_SCHEMA = 'DEMO';


-- Выполним более сложный запрос с объединением трех представлений INFORMATION_SCHEMA для определения состава,
-- типа и признаков вхождения в первичный ключ и ключ шардирования каждой из колонок по заданной таблице

SELECT c.table_schema,
       c.table_name,
       c.column_name,
       c.data_type,
       c.character_maximum_length,
       c.datetime_precision,
       u.ordinal_position,
       con.constraint_type
FROM INFORMATION_SCHEMA.COLUMNS c
         LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE u
                         ON u.table_schema = c.table_schema
                             AND u.table_name = c.table_name
                             AND u.column_name = c.column_name
         LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS con
                         ON con.constraint_schema = u.constraint_schema
                             AND con.table_schema = u.table_schema
                             AND con.constraint_catalog = u.constraint_catalog
                             AND con.table_name = u.table_name
WHERE c.table_schema = 'DEMO'
  AND c.table_name = 'TRANSACTIONS';

--  1. Запустим 100 одновременных операций вида DROP TABLE через 100 подключений
--  2. Продемонстрировать отсутствие таблиц в хранилище (ADB, ADG, ADQM)
--  3. Продемонстрируем состояние мета-данных в Zookeeper http://10.92.3.3:10001/node?path=/adtm/orig/demo/entity
--  4. Убедимся в корректном отображении схемы данных в системных представлениях
select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'DEMO';
select * from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = 'DEMO';


-- 1. Создать датамарт
-- 2. Создать таблицу счетов accounts
create table demo.accounts
(
    account_id bigint,
    account_type varchar(1),
    primary key (account_id)
) distributed by (account_id);

-- 2. Создать таблицу транзакций transactions
create table demo.transactions
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint,
    primary key (transaction_id)
) distributed by (transaction_id);

-- 3. Загрузить в accounts.topicY данные Эмулятором Поставщика (10 строк)
-- 4. Загрузить в transactions.topicY данные Эмулятором Поставщика (1000 строк)

-- 5.  Создать внешнюю таблицу загрузки accounts_ext (topic = accounts.topicY)
CREATE UPLOAD EXTERNAL TABLE accounts_ext
(
    account_id bigint,
    account_type varchar(1)
)
LOCATION 'kafka://10.92.3.25:2181/ACCOUNTS_TOPIC_Y'
FORMAT 'AVRO';

-- 6. Создать внешнюю таблицу загрузки transactions_ext topic = transactions.topicY)
CREATE UPLOAD EXTERNAL TABLE transactions_ext
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint
)
LOCATION 'kafka://10.92.3.25:2181/TRANSACTIONS_TOPIC_Y'
FORMAT 'AVRO';

BEGIN DELTA;

-- 7. Выполнить вставку данных в таблицу счетов INSERT INTO accounts SELECT * FROM accounts_ext
INSERT INTO accounts SELECT * FROM accounts_ext;
-- 8. Выполнить вставку данных в таблицу транзакций
--      a. Запустить команду INSERT INTO transactions SELECT * FROM transactions_ext
INSERT INTO transactions SELECT * FROM transactions_ext;

-- Продемонстрировать наличие данных в таблице счетом (10) в незакомиченной дельте
-- Продемонстрировать наличие данных в таблице транзакций (1000) в незакомиченной дельте
COMMIT DELTA;

--  Продемонстрировать наличие данных в таблице счетом (10) в нзакомиченной дельте
--  Продемонстрировать наличие данных в таблице транзакций (1000) в закомиченной дельте


--  В процессе загрузки второй дельты искусственно создадим ошибочную ситуацию, которая не позволит успешно завершить загрузку.
BEGIN DELTA;
-- 5. Выполнить вставку данных в таблицу счетов INSERT INTO accounts SELECT * FROM accounts_ext
INSERT INTO accounts SELECT * FROM accounts_ext;
-- 6. Выполнить вставку данных в таблицу транзакций
--      a. Запустить команду INSERT INTO transactions SELECT * FROM transactions_ext
INSERT INTO transactions SELECT * FROM transactions_ext;

-- Создать новую внешнюю таблицу transactions_ext_2 (topic = transactions.topicY_2)
CREATE UPLOAD EXTERNAL TABLE transactions_ext_2
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint
)
LOCATION 'kafka://10.92.3.25:2181/TRANSACTIONS_TOPIC_Y_2'
FORMAT 'AVRO';

-- Выполнить вставку данных в таблицу транзакций INSERT INTO transactions SELECT * FROM transactions_ext_2
INSERT INTO transactions SELECT * FROM transactions_ext_2;
-- Продемонстрировать наличие новых данных в таблице транзакций в незакомиченной дельте (4000 строк)

COMMIT DELTA;

-- Продемонстрировать выборку данных из таблицы счетов (30)
SELECT COUNT(*) FROM accounts DATASOURCE_TYPE = 'ADB';
SELECT COUNT(*) FROM accounts DATASOURCE_TYPE = 'ADQM';
SELECT COUNT(*) FROM accounts DATASOURCE_TYPE = 'ADG';

-- Продемонстрировать выборку данных из таблицы транзакций (4000 строк)
SELECT COUNT(*) FROM transactions DATASOURCE_TYPE = 'ADB';
SELECT COUNT(*) FROM transactions DATASOURCE_TYPE = 'ADQM';
SELECT COUNT(*) FROM transactions DATASOURCE_TYPE = 'ADG';

-- drop database demo;
