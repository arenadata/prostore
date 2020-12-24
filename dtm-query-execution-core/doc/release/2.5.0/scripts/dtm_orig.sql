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
create database demomvp

create table demomvp.accounts
(
    account_id bigint,
    account_type varchar(1), -- D/C (дебет/кредит)
    primary key (account_id)
) distributed by (account_id);

USE demomvp;

create table transactions
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint,
    primary key (transaction_id)
) distributed by (transaction_id);

--------------------- step 2 ---------------------

CREATE UPLOAD EXTERNAL TABLE accounts_ext
(
    account_id bigint,
    account_type varchar(1)
)
LOCATION 'kafka://10.92.3.25:2181/DEMOMVP_ACCOUNTS_EXT'
FORMAT 'AVRO';


CREATE UPLOAD EXTERNAL TABLE transactions_ext
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint
)
LOCATION 'kafka://10.92.3.25:2181/DEMOMVP_TRANSACTIONS_EXT'
FORMAT 'AVRO';

--------------------- load delta0 in kafka ---------------------
BEGIN DELTA;
	select * from information_schema.DELTAS where delta_schema = 'demomvp';


	INSERT INTO accounts select * from accounts_ext;

	INSERT INTO transactions select * from transactions_ext;
COMMIT DELTA;

select count(*) from demomvp.accounts DATASOURCE_TYPE = 'ADB';
select count(*) from demomvp.accounts DATASOURCE_TYPE = 'ADG';
select count(*) from demomvp.accounts DATASOURCE_TYPE = 'ADQM';
select count(*) from demomvp.transactions DATASOURCE_TYPE = 'ADB';
select count(*) from demomvp.transactions DATASOURCE_TYPE = 'ADG';
select count(*) from demomvp.transactions DATASOURCE_TYPE = 'ADQM';

select * from demomvp.transactions;


--------------------- step 3-4 ---------------------
--------------------- replication.rq.avro ---------------------
--------------------- replication.in.avro ---------------------

--------------------- step 5 ---------------------
--------------------- load delta1 in kafka ---------------------
BEGIN DELTA;

	INSERT INTO transactions select * from transactions_ext;

COMMIT DELTA

select count(*) from transactions DATASOURCE_TYPE = 'ADB';
select count(*) from demomvp.transactions DATASOURCE_TYPE = 'ADG';
select count(*) from demomvp.transactions DATASOURCE_TYPE = 'ADQM';

--------------------- step 6 ---------------------
--------------------- load delta2 in kafka ---------------------
BEGIN DELTA

	INSERT INTO transactions select * from transactions_ext;

COMMIT DELTA

select count(*) from transactions DATASOURCE_TYPE = 'ADB';
select count(*) from transactions DATASOURCE_TYPE = 'ADG';
select count(*) from transactions DATASOURCE_TYPE = 'ADQM';

--------------------- step 7 ---------------------
--------------------- delta1.rq.avro ---------------------

--------------------- step 8 ---------------------
--------------------- delta1.rq.avro ---------------------


