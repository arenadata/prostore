CREATE SCHEMA datamart;

CREATE TABLE datamart.test_table (
id int8 NOT NULL,
varchar_col varchar(10),
char_col varchar(10),
bigint_col int8,
int_col int8,
int32_col int4,
double_col float8,
float_col float4,
date_col date,
time_col time(6),
timestamp_col timestamp(6),
boolean_col bool,
uuid_col varchar(36),
link_col varchar,
constraint pk_datamart_table_actual primary key (id));

insert into datamart.test_table
(id, varchar_col, char_col, bigint_col, int_col, int32_col, double_col, float_col, date_col, time_col, timestamp_col, boolean_col, uuid_col, link_col)
values
(12, 'varchar', 'char', 11, 12, 13, 1.4, 1.5, '2017-03-14', '00:00:00.000001', '2016-06-22 19:10:25', true, 'uuid', 'link');
