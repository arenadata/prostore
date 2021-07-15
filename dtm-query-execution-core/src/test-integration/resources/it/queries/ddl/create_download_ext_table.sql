--
-- Copyright © 2021 ProStore
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

create download external table %s.%s
(
  id int not null,
  double_col double,
  float_col float,
  varchar_col varchar(36),
  boolean_col boolean,
  int_col int,
  bigint_col bigint,
  date_col date,
  timestamp_col timestamp,
  time_col time(5),
  uuid_col uuid,
  char_col char(10)
)
LOCATION 'kafka://%s/%s'
FORMAT 'AVRO';