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

CREATE DATABASE IF NOT EXISTS test__shares ON CLUSTER test_arenadata;

----------------

create table test__shares.accounts_actual_shard on cluster test_arenadata
(
    account_type Nullable(FixedString(1)),
    account_id   Int64,
    sys_from     Int64,
    sys_to       Nullable(Int64),
    sign         Int8
)
    ENGINE CollapsingMergeTree(sign)
        order by (account_id, sys_from)
;
CREATE TABLE test__shares.accounts_actual  on cluster test_arenadata
(
    account_type Nullable(FixedString(1)),
    account_id   Int64,
    sys_from     Int64,
    sys_to       Nullable(Int64),
    sign         Int8
)
    ENGINE Distributed(test_arenadata, test__shares, accounts_actual_shard, account_id)
;
create table test__shares.transactions_actual_shard on cluster test_arenadata
(
    transaction_id   Int64,
    transaction_date DateTime64(3),
    account_id       Int64,
    amount           Int64,
    sys_from         Int64,
    sys_to           Nullable(Int64),
    sign             Int8
)
    ENGINE CollapsingMergeTree(sign)
        order by (transaction_id, sys_from)
;
CREATE TABLE test__shares.transactions_actual  on cluster test_arenadata
(
    transaction_id   Int64,
    transaction_date DateTime64(3),
    account_id       Int64,
    amount           Int64,
    sys_from         Int64,
    sys_to           Nullable(Int64),
    sign             Int8
)
    ENGINE Distributed(test_arenadata, test__shares, transactions_actual_shard, account_id)
;


insert into test__shares.accounts_actual_shard values ('C', 1, 1, 1, 0);
insert into test__shares.accounts_actual_shard values ('D', 2, 1, 1, 0);
insert into test__shares.accounts_actual_shard values ('D', 3, 1, 1, 0);
insert into test__shares.transactions_actual_shard values(1, '2020-06-11', 1, -100, 0, 1, 0);  --проводка 1
insert into test__shares.transactions_actual_shard values(1, '2020-06-11', 2,  100, 0, 1, 0);  --проводка 1
insert into test__shares.transactions_actual_shard values(2, '2020-06-12', 2,  -50, 1, 1, 0);  --проводка 2
insert into test__shares.transactions_actual_shard values(2, '2020-06-12', 3,   50, 1, 1, 0);  --проводка 2
insert into test__shares.transactions_actual_shard values(3, '2020-06-12', 3,  -20, 1, 1, 0);  --проводка 3
insert into test__shares.transactions_actual_shard values(3, '2020-06-12', 1,   20, 1, 1, 0);  --проводка 3

