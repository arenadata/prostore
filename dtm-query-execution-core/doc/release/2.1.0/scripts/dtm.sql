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

CREATE DATABASE shares

create table shares.accounts
(
    account_id bigint,
    account_type varchar(1), -- D/C (дебет/кредит)
    primary key (account_id)
) distributed by (account_id);

create table shares.transactions
(
    transaction_id bigint,
    transaction_date date,
    account_id bigint,
    amount bigint,
    primary key (transaction_id, account_id)
) distributed by (account_id);

select * from shares.accounts;

BEGIN DELTA
-- insert into shares.accounts values
-- (1, 'C'),
-- (2, 'D'),
-- (3, 'D');

-- insert into shares.transactions values(1, '2020-06-11', 1, -100);  --проводка 1
-- insert into shares.transactions values(1, '2020-06-11', 2,  100);  --проводка 1
COMMIT DELTA


BEGIN DELTA
-- insert into shares.transactions values(3, '2020-06-12', 3,  -20);  --проводка 3
-- insert into shares.transactions values(3, '2020-06-12', 1,   20);  --проводка 3

-- check

-- insert into shares.transactions values(2, '2020-06-12', 2,  -50);  --проводка 2
-- insert into shares.transactions values(2, '2020-06-12', 3,   50);  --проводка 2
COMMIT DELTA

---- ====================================

select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END
  from (
    select a.account_id, coalesce(sum(amount),0) amount, account_type
    from shares.accounts a
    left join shares.transactions t using(account_id)
   group by a.account_id, account_type
)x ;


select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END
  from (
    select a.account_id, coalesce(sum(amount),0) amount, account_type
    from shares.accounts a
    left join shares.transactions t using(account_id)
   group by a.account_id, account_type
)x DATASOURCE_TYPE = 'ADB';

select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END
  from (
    select a.account_id, coalesce(sum(amount),0) amount, account_type
    from shares.accounts a
    left join shares.transactions FOR SYSTEM_TIME AS OF '2020-06-30 16:18:58' t using(account_id)
   group by a.account_id, account_type
)x DATASOURCE_TYPE = 'ADB';

create view v_accounts_deb as SELECT * from shares.accounts where account_type = 'D';

select * from shares.v_accounts_deb;

select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END
  from (
    select a.account_id, coalesce(sum(amount),0) amount, account_type
    from shares.v_accounts_deb a
    left join shares.transactions t using(account_id)
   group by a.account_id, account_type
)x ;

select * from shares.transactions DATASOURCE_TYPE = 'ADG';

select * from shares.transactions FOR SYSTEM_TIME AS OF '2020-06-30 16:18:58' DATASOURCE_TYPE = 'ADQM';
