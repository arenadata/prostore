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
---
SELECT * FROM (SELECT t11.account_id FROM (SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t6 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t11 INNER JOIN local__shares.transactions_actual_shard FINAL ON t11.account_id = transactions_actual_shard.account_id WHERE transactions_actual_shard.sys_from <= 1 AND (transactions_actual_shard.sys_to >= 1 AND t11.account_id = 1)) AS t13 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT t30.account_id FROM (SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t19 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t25 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t30 INNER JOIN local__shares.transactions_actual_shard AS transactions_actual_shard1 ON t30.account_id = transactions_actual_shard1.account_id WHERE transactions_actual_shard1.sys_from <= 1 AND (transactions_actual_shard1.sys_to >= 1 AND t30.account_id = 1)) AS t32 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL
---
SELECT * FROM (SELECT accounts_actual.account_id FROM local__shares.accounts_actual FINAL INNER JOIN local__shares.transactions_actual_shard FINAL ON accounts_actual.account_id = transactions_actual_shard.account_id WHERE accounts_actual.sys_from <= 1 AND accounts_actual.sys_to >= 1 AND (transactions_actual_shard.sys_from <= 1 AND (transactions_actual_shard.sys_to >= 1 AND accounts_actual.account_id = 10))) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL AND (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT accounts_actual1.account_id FROM local__shares.accounts_actual AS accounts_actual1 INNER JOIN local__shares.transactions_actual_shard AS transactions_actual_shard1 ON accounts_actual1.account_id = transactions_actual_shard1.account_id WHERE accounts_actual1.sys_from <= 1 AND accounts_actual1.sys_to >= 1 AND (transactions_actual_shard1.sys_from <= 1 AND (transactions_actual_shard1.sys_to >= 1 AND accounts_actual1.account_id = 10))) AS t9 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL OR (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL
---
SELECT * FROM (SELECT accounts_actual.account_id, accounts_actual.account_type, transactions_actual_shard.transaction_id, transactions_actual_shard.transaction_date, transactions_actual_shard.account_id AS account_id0, transactions_actual_shard.amount FROM local__shares.accounts_actual FINAL INNER JOIN local__shares.transactions_actual_shard FINAL ON accounts_actual.account_id = transactions_actual_shard.account_id WHERE accounts_actual.sys_from <= 1 AND accounts_actual.sys_to >= 1 AND (transactions_actual_shard.sys_from <= 1 AND transactions_actual_shard.sys_to >= 1)) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL AND (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT accounts_actual1.account_id, accounts_actual1.account_type, transactions_actual_shard1.transaction_id, transactions_actual_shard1.transaction_date, transactions_actual_shard1.account_id AS account_id0, transactions_actual_shard1.amount FROM local__shares.accounts_actual AS accounts_actual1 INNER JOIN local__shares.transactions_actual_shard AS transactions_actual_shard1 ON accounts_actual1.account_id = transactions_actual_shard1.account_id WHERE accounts_actual1.sys_from <= 1 AND accounts_actual1.sys_to >= 1 AND (transactions_actual_shard1.sys_from <= 1 AND transactions_actual_shard1.sys_to >= 1)) AS t9 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL OR (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL
---
SELECT account_id, CASE WHEN SUM(amount) IS NOT NULL THEN SUM(amount) ELSE 0 END AS __f1, account_type, CASE WHEN account_type = 'D' AND CASE WHEN SUM(amount) IS NOT NULL THEN SUM(amount) ELSE 0 END >= 0 OR account_type = 'C' AND CASE WHEN SUM(amount) IS NOT NULL THEN SUM(amount) ELSE 0 END <= 0 THEN 'OK' ELSE 'NOT OK' END AS __f3 FROM (SELECT * FROM (SELECT accounts_actual.account_id, accounts_actual.account_type, transactions_actual_shard.amount FROM local__shares.accounts_actual FINAL LEFT JOIN local__shares.transactions_actual_shard FINAL ON accounts_actual.account_id = transactions_actual_shard.account_id WHERE accounts_actual.sys_from <= 1 AND accounts_actual.sys_to >= 1 AND (transactions_actual_shard.sys_from <= 1 AND transactions_actual_shard.sys_to >= 1)) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL AND (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT accounts_actual1.account_id, accounts_actual1.account_type, transactions_actual_shard1.amount FROM local__shares.accounts_actual AS accounts_actual1 LEFT JOIN local__shares.transactions_actual_shard AS transactions_actual_shard1 ON accounts_actual1.account_id = transactions_actual_shard1.account_id WHERE accounts_actual1.sys_from <= 1 AND accounts_actual1.sys_to >= 1 AND (transactions_actual_shard1.sys_from <= 1 AND transactions_actual_shard1.sys_to >= 1)) AS t9 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL OR (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL) AS t17 GROUP BY account_id, account_type
---
SELECT * FROM (SELECT transaction_id, transaction_date, account_id, amount FROM local__shares.transactions_actual FINAL WHERE sys_from <= 1 AND sys_to >= 1) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT transaction_id, transaction_date, account_id, amount FROM local__shares.transactions_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t6 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual WHERE sign < 0 LIMIT 1))) IS NULL
---
SELECT * FROM (SELECT t11.account_id, transactions_actual_shard.transaction_id, transactions_actual_shard.transaction_date, transactions_actual_shard.account_id AS account_id0, transactions_actual_shard.amount FROM (SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t6 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t11 INNER JOIN local__shares.transactions_actual_shard FINAL ON t11.account_id = transactions_actual_shard.account_id WHERE transactions_actual_shard.sys_from <= 1 AND transactions_actual_shard.sys_to >= 1) AS t13 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT t30.account_id, transactions_actual_shard1.transaction_id, transactions_actual_shard1.transaction_date, transactions_actual_shard1.account_id AS account_id0, transactions_actual_shard1.amount FROM (SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t19 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t25 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t30 INNER JOIN local__shares.transactions_actual_shard AS transactions_actual_shard1 ON t30.account_id = transactions_actual_shard1.account_id WHERE transactions_actual_shard1.sys_from <= 1 AND transactions_actual_shard1.sys_to >= 1) AS t32 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL
---
SELECT * FROM (SELECT t11.account_id FROM (SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t6 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t11 INNER JOIN local__shares.transactions_actual_shard FINAL ON t11.account_id = transactions_actual_shard.account_id INNER JOIN local__shares.transactions_actual_shard FINAL AS transactions_actual_shard0 ON t11.account_id = transactions_actual_shard0.transaction_id INNER JOIN local__shares.transactions_actual_shard FINAL AS transactions_actual_shard1 ON t11.account_id = transactions_actual_shard1.transaction_id WHERE transactions_actual_shard.sys_from <= 1 AND transactions_actual_shard.sys_to >= 1 AND (transactions_actual_shard0.sys_from <= 1 AND transactions_actual_shard0.sys_to >= 1) AND (transactions_actual_shard1.sys_from <= 1 AND transactions_actual_shard1.sys_to >= 1 AND (transactions_actual_shard.account_id = 5 AND transactions_actual_shard0.transaction_id = 3))) AS t13 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT t30.account_id FROM (SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t19 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 12)) AS t25 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t30 INNER JOIN local__shares.transactions_actual_shard AS transactions_actual_shard3 ON t30.account_id = transactions_actual_shard3.account_id INNER JOIN local__shares.transactions_actual_shard AS transactions_actual_shard4 ON t30.account_id = transactions_actual_shard4.transaction_id INNER JOIN local__shares.transactions_actual_shard AS transactions_actual_shard5 ON t30.account_id = transactions_actual_shard5.transaction_id WHERE transactions_actual_shard3.sys_from <= 1 AND transactions_actual_shard3.sys_to >= 1 AND (transactions_actual_shard4.sys_from <= 1 AND transactions_actual_shard4.sys_to >= 1) AND (transactions_actual_shard5.sys_from <= 1 AND transactions_actual_shard5.sys_to >= 1 AND (transactions_actual_shard3.account_id = 5 AND transactions_actual_shard4.transaction_id = 3))) AS t32 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL
---
SELECT account_id FROM (SELECT * FROM (SELECT accounts_actual.account_id FROM local__shares.accounts_actual FINAL INNER JOIN local__shares.transactions_actual_shard FINAL ON accounts_actual.account_id = transactions_actual_shard.account_id WHERE accounts_actual.sys_from <= 1 AND accounts_actual.sys_to >= 1 AND (transactions_actual_shard.sys_from <= 1 AND transactions_actual_shard.sys_to >= 1)) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL AND (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT accounts_actual1.account_id FROM local__shares.accounts_actual AS accounts_actual1 INNER JOIN local__shares.transactions_actual_shard AS transactions_actual_shard1 ON accounts_actual1.account_id = transactions_actual_shard1.account_id WHERE accounts_actual1.sys_from <= 1 AND accounts_actual1.sys_to >= 1 AND (transactions_actual_shard1.sys_from <= 1 AND transactions_actual_shard1.sys_to >= 1)) AS t9 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL OR (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL) AS t LIMIT 10
---
SELECT COUNT(*) AS EXPR__0 FROM (SELECT * FROM (SELECT 0 AS __f0 FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND sys_to >= 1) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT 0 AS __f0 FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t6 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t11
---
SELECT * FROM (SELECT transaction_id, transaction_date, account_id, amount FROM local__shares.transactions_actual FINAL WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 1)) AS t0 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT transaction_id, transaction_date, account_id, amount FROM local__shares.transactions_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 1)) AS t6 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual WHERE sign < 0 LIMIT 1))) IS NULL
