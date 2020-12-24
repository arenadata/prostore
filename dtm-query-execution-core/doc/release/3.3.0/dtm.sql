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

-- drop database december2020;
create database december2020;
use december2020;

-- Создать таблицу accounts
create table december2020.accounts
(
    account_id bigint,
    account_type varchar(1),
    primary key (account_id)
) distributed by (account_id);

-- Создать таблицу transactions
create table december2020.transactions
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint,
    primary key (transaction_id, account_id)
) distributed by (account_id);

-- Создать представление transactions_view вида
CREATE VIEW transactions_view
AS SELECT a.account_type, t.transaction_id, t.transaction_date, t.amount
   FROM transactions t
            INNER JOIN accounts a ON t.account_id = a.account_id;

-- Выполнить выборку из системных представлений, демонстрирующих созданные таблицы и представление

select * from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'DECEMBER2020';
select * from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'DECEMBER2020';

-- Создать внешнюю таблицу загрузки accounts_ext (topic = accounts.topicY)
CREATE UPLOAD EXTERNAL TABLE accounts_ext
(
    account_id bigint,
    account_type varchar(1)
)
LOCATION 'kafka://10.92.3.25:2181/ACCOUNTS_TOPIC_DECEMBER2020_Z'
FORMAT 'AVRO';

-- Создать внешнюю таблицу загрузки transactions_ext topic = transactions.topicY)
CREATE UPLOAD EXTERNAL TABLE transactions_ext1
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint
)
LOCATION 'kafka://10.92.3.25:2181/TRANSACTIONS_TOPIC_DECEMBER2020_Z1'
FORMAT 'AVRO';

-- Выполнить вставку данных

begin delta;
insert into accounts select * from accounts_ext;
insert into transactions select * from transactions_ext1;
commit delta;

-- select count(*) from transactions DATASOURCE_TYPE = 'adqm';
-- Выполнить запросы вида
SELECT count(*) FROM transactions_view DATASOURCE_TYPE = 'adb'; -- (100k)
SELECT count(*) FROM transactions_view DATASOURCE_TYPE = 'adqm'; -- (100k) (50k!!!)
SELECT count(*) FROM transactions_view DATASOURCE_TYPE = 'adg'; -- (100k)

-- Загрузить в transactions.topicY данные Эмулятором Поставщика (100k строк из них 50k имеют id из предыдущего набора)
CREATE UPLOAD EXTERNAL TABLE transactions_ext2
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint
)
LOCATION 'kafka://10.92.3.25:2181/TRANSACTIONS_TOPIC_DECEMBER2020_Z2'
FORMAT 'AVRO';

begin delta;
insert into transactions select * from transactions_ext2;
commit delta;

-- Выполнить запросы вида
SELECT count(*) FROM transactions DATASOURCE_TYPE = 'adb'; -- (150k)
SELECT count(*) FROM transactions DATASOURCE_TYPE = 'adqm'; -- (150k)
SELECT count(*) FROM transactions DATASOURCE_TYPE = 'adg'; -- (150k)

SELECT count(*) FROM transactions FOR SYSTEM_TIME AS OF  '2020-12-10 18:50:31' DATASOURCE_TYPE = 'adb'; -- (100k)
SELECT count(*) FROM transactions FOR SYSTEM_TIME AS OF  '2020-12-10 18:50:31' DATASOURCE_TYPE = 'adqm'; -- (100k)
SELECT count(*) FROM transactions FOR SYSTEM_TIME AS OF  '2020-12-10 18:50:31' DATASOURCE_TYPE = 'adg'; -- (100k)

-- LLR Выполнить 1000 параллельных запросов

-- MMP-R
-- Проверить количество счетов типа D запросом вида:
SELECT count(*) FROM accounts WHERE account_type = 'D' -- (xd)

-- Последовательно сконфигурировать ядро для выгрузки для каждой из БД (ADB, ADQM, ADG) и выполнить следующие шаги

-- ADB
create DOWNLOAD external table december2020.transactions_out_adb
(
    account_type varchar(1),
    transaction_id bigint not null,
    transaction_date varchar(20),
    amount bigint
)
LOCATION 'kafka://10.92.3.25:2181/DOWNLOAD_TRANSACTIONS_OUT_DECEMBER2020_Z_ADB'
FORMAT 'AVRO';

-- ADG
create DOWNLOAD external table december2020.transactions_out_adg
(
    account_type varchar(1),
    transaction_id bigint not null,
    transaction_date varchar(20),
    amount bigint
)
LOCATION 'kafka://10.92.3.25:2181/DOWNLOAD_TRANSACTIONS_OUT_DECEMBER2020_Z_ADG'
FORMAT 'AVRO';

-- ADQM
create DOWNLOAD external table december2020.transactions_out_adqm
(
    account_type varchar(1),
    transaction_id bigint not null,
    transaction_date varchar(20),
    amount bigint
)
LOCATION 'kafka://10.92.3.25:2181/DOWNLOAD_TRANSACTIONS_OUT_DECEMBER2020_Z_ADQM'
FORMAT 'AVRO';
--
INSERT INTO transactions_out_adb SELECT * FROM transactions_view WHERE account_type = 'D';
INSERT INTO transactions_out_adqm SELECT * FROM transactions_view WHERE account_type = 'D';
INSERT INTO transactions_out_adg SELECT * FROM transactions_view WHERE account_type = 'D';

-- ROLLBACK
-- Загрузить в transactions.topicY данные Эмулятором Поставщика (77k строк из них 33k имеют id из предыдущего набора)
CREATE UPLOAD EXTERNAL TABLE transactions_ext3
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint
)
LOCATION 'kafka://10.92.3.25:2181/TRANSACTIONS_TOPIC_DECEMBER2020_Z3'
FORMAT 'AVRO';
begin delta;
insert into transactions select * from transactions_ext3;

SELECT count(*) FROM transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA DATASOURCE_TYPE = 'adb'; -- (199k)
SELECT count(*) FROM transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA DATASOURCE_TYPE = 'adqm'; -- (199k)
SELECT count(*) FROM transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA DATASOURCE_TYPE = 'adg'; -- (199k)

rollback delta;

SELECT count(*) FROM transactions DATASOURCE_TYPE = 'adb'; -- (150k)
SELECT count(*) FROM transactions DATASOURCE_TYPE = 'adqm'; -- (150k)
SELECT count(*) FROM transactions DATASOURCE_TYPE = 'adg'; -- (150k)

SELECT count(*) FROM transactions FOR SYSTEM_TIME AS OF '2020-12-10 18:50:31' DATASOURCE_TYPE = 'adb'; -- (100k)
SELECT count(*) FROM transactions FOR SYSTEM_TIME AS OF '2020-12-10 18:50:31' DATASOURCE_TYPE = 'adqm'; -- (100k)
SELECT count(*) FROM transactions FOR SYSTEM_TIME AS OF '2020-12-10 18:50:31' DATASOURCE_TYPE = 'adg'; -- (100k)

-- CHECK_DATA
CHECK_DATA(transactions, 1); -- (ok)
CHECK_DATA(transactions, 1, [transaction_id, account_id, amount]); -- (ok)

-- Вручную изменить amount в ADB, ADG для одной из записей в таблице transactions_actual
CHECK_DATA(transactions, 1, [transaction_id, account_id, amount]); -- (not ok)

-- TRUNCATE HISTORY
SELECT * FROM transactions WHERE transaction_id = 1; -- (1)
SELECT * FROM transactions FOR SYSTEM_TIME AS OF '2020-12-10 18:50:31' WHERE transaction_id = 1; -- (1)

TRUNCATE HISTORY transactions FOR SYSTEM_TIME AS OF 'infinite' WHERE transaction_id = 1;

SELECT * FROM transactions WHERE transaction_id = 1 -- (0)
SELECT * FROM transactions FOR SYSTEM_TIME AS OF '2020-12-10 18:50:31' WHERE transaction_id = 1 -- (0)

-- Продемонстрировать отсутствие записи в ADB, ADG, ADQM с transaction_id = 1, в том числе для таблиц с историей

