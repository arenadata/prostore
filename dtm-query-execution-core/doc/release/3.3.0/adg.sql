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

UPDATE "demo__december2020__transactions_actual" SET "amount" = 10 where "transaction_id" = 55555;
--
SELECT * FROM "demo__december2020__transactions_actual" where "transaction_id" = 1;
SELECT * FROM "demo__december2020__transactions_history" where "transaction_id" = 1;
