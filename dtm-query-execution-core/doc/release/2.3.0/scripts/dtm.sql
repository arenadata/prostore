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


--------------------- step 1 ---------------------
create database demo_release2

create table demo_release2.accounts
(
    account_id bigint,
    account_type varchar(1), -- D/C (дебет/кредит)
    primary key (account_id)
) distributed by (account_id);

select * from demo_release2.accounts;

--------------------- step 2 ---------------------
drop table demo_release2.accounts;

--------------------- step 3 ---------------------
-- Load data to kafka for MPP-W

--------------------- step 4 ---------------------
create table demo_release2.accounts
(
    account_id bigint,
    account_type varchar(1), -- D/C (дебет/кредит)
    primary key (account_id)
) distributed by (account_id);

create table demo_release2.transactions
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint,
    primary key (transaction_id, account_id, amount)
) distributed by (account_id);

CREATE UPLOAD EXTERNAL TABLE demo_release2.accounts_ext
(
    account_id bigint,
    account_type varchar(1)
)
LOCATION 'kafka://10.92.6.44:2181/DEMO_RELEASE2_ACCOUNTS_EXT'
FORMAT 'AVRO'

CREATE UPLOAD EXTERNAL TABLE demo_release2.transactions_ext
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint
)
LOCATION 'kafka://10.92.6.44:2181/DEMO_RELEASE2_TRANSACTIONS_EXT'
FORMAT 'AVRO'

--------------------- step 5 ---------------------
BEGIN DELTA
select * from information_schema.deltas where delta_schema = 'demo_release2';

INSERT INTO demo_release2.accounts SELECT * FROM demo_release2.accounts_ext;
INSERT INTO demo_release2.transactions select * from demo_release2.transactions_ext;

COMMIT DELTA

--------------------- step 6 ---------------------
select * from demo_release2.accounts FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA  DATASOURCE_TYPE = 'ADG';
select * from demo_release2.transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA DATASOURCE_TYPE = 'ADG';

select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' end
FROM (
    select account_id, coalesce(sum(amount),0) amount, account_type
    from demo_release2.accounts left join demo_release2.transactions  using(account_id)
    group by account_id, account_type
)x DATASOURCE_TYPE = 'ADQM';

--------------------- step 7 ---------------------
BEGIN DELTA
-- Load data to kafka for MPP-W
INSERT INTO demo_release2.transactions select * from demo_release2.transactions_ext;

select * from transactions DATASOURCE_TYPE = 'ADQM';
select * from demo_release2.transactions DATASOURCE_TYPE = 'ADB';

--------------------- step 8 ---------------------
select * from demo_release2.transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA DATASOURCE_TYPE = 'ADG';

--------------------- step 9 ---------------------
select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' end
FROM (
    select account_id, coalesce(sum(amount),0) amount, account_type
    from demo_release2.accounts left join demo_release2.transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA using(account_id)
    group by account_id, account_type
)x DATASOURCE_TYPE = 'ADQM';

-- Load data to kafka for MPP-W transaction 2
INSERT INTO demo_release2.transactions select * from demo_release2.transactions_ext;

select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' end
FROM (
    select account_id, coalesce(sum(amount),0) amount, account_type
    from demo_release2.accounts left join demo_release2.transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA using(account_id)
    group by account_id, account_type
)x DATASOURCE_TYPE = 'ADQM';

select *
from demo_release2.transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA
DATASOURCE_TYPE = 'ADG';

--------------------- step 10 ---------------------
COMMIT DELTA
select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' end
FROM (
    select account_id, coalesce(sum(amount),0) amount, account_type
    from demo_release2.accounts left join demo_release2.transactions  using(account_id)
    group by account_id, account_type
)x DATASOURCE_TYPE = 'ADB';
DROP UPLOAD EXTERNAL TABLE demo_release2.accounts_ext;
DROP UPLOAD EXTERNAL TABLE demo_release2.transactions_ext;

--------------------- step 11 ---------------------
select * from information_schema.deltas where delta_schema = 'demo_release2';

select *
from demo_release2.transactions FOR SYSTEM_TIME AS OF '2020-07-23 18:42:50'
DATASOURCE_TYPE = 'ADB';

create view demo_release2.myview as
select *
from demo_release2.transactions FOR SYSTEM_TIME finished in (0,1)
DATASOURCE_TYPE = 'ADG';

select * from demo_release2.myview where account_id = 1;


select * from demo_release2.transactions FOR SYSTEM_TIME AS OF '2020-07-23 18:42:50'

select *
from demo_release2.transactions FOR SYSTEM_TIME as of delta_num 1
DATASOURCE_TYPE = 'ADB';


