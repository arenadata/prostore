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

insert into shares.accounts_actual (account_id, account_type, sys_from, sys_op)
values (1, 'C', 0, 1),
       (2, 'D', 0, 1),
       (3, 'D', 0, 1);

-- DELTA BEGIN
-- insert into shares.transactions values(1, '2020-06-11', 1, -100);  --проводка 1
-- insert into shares.transactions values(1, '2020-06-11', 2,  100);  --проводка 1
-- DELTA COMMIT
insert into shares.transactions_actual (transaction_id, transaction_date, account_id, amount, sys_from, sys_to, sys_op)
    values (1, '2020-06-11', 1, -100, 0, null, 0); --проводка 1
insert into shares.transactions_actual (transaction_id, transaction_date, account_id, amount, sys_from, sys_to, sys_op)
    values (1, '2020-06-11', 2, 100, 0, null, 0); --проводка 1


-- DELTA BEGIN
-- insert into shares.transactions values(3, '2020-06-12', 3,  -20);  --проводка 3
-- insert into shares.transactions values(3, '2020-06-12', 1,   20);  --проводка 3

-- insert into shares.transactions values(2, '2020-06-12', 2,  -50);  --проводка 2
-- insert into shares.transactions values(2, '2020-06-12', 3,   50);  --проводка 2

-- DELTA COMMIT
insert into shares.transactions_actual (transaction_id, transaction_date, account_id, amount, sys_from, sys_to, sys_op)
values (3, '2020-06-12', 3, -20, 1, null, 0); --проводка 3
insert into shares.transactions_actual (transaction_id, transaction_date, account_id, amount, sys_from, sys_to, sys_op)
values (3, '2020-06-12', 1, 20, 1, null, 0); --проводка 3
insert into shares.transactions_actual (transaction_id, transaction_date, account_id, amount, sys_from, sys_to, sys_op)
values (2, '2020-06-12', 2, -50, 1, null, 0); --проводка 2
insert into shares.transactions_actual (transaction_id, transaction_date, account_id, amount, sys_from, sys_to, sys_op)
values (2, '2020-06-12', 3, 50, 1, null, 0); --проводка 2

select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END
from (
         select a.account_id, coalesce(sum(amount),0) amount, account_type
         from shares.accounts_actual a
                  left join shares.transactions_actual t using(account_id)
         group by a.account_id, account_type
     )x ;