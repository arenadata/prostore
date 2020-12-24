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


use demomvp;

select * from accounts;
select * from transactions;


select * from information_schema.DELTAS where delta_schema = 'demomvp';

select count(*) from accounts DATASOURCE_TYPE = 'ADB';
select count(*) from accounts DATASOURCE_TYPE = 'ADG';
select count(*) from accounts DATASOURCE_TYPE = 'ADQM';
select count(*) from transactions DATASOURCE_TYPE = 'ADB';
select count(*) from transactions DATASOURCE_TYPE = 'ADG';
select count(*) from transactions DATASOURCE_TYPE = 'ADQM';

-- commit delta;

select count(*) from transactions for system_time as of latest_uncommitted_delta DATASOURCE_TYPE = 'ADB';

select count(*) from transactions;

select count(*) from transactions FOR SYSTEM_TIME as of delta_num 1;

select count(*) from transactions FOR SYSTEM_TIME as of delta_num 1 DATASOURCE_TYPE = 'ADQM';

select count(*) from transactions FOR SYSTEM_TIME as of '' DATASOURCE_TYPE = 'ADG';
