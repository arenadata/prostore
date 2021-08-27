### Prostore 5.1.0, 2021-08-27

#### New functionality
* Estimation of an enriched LL-R query by using the ESTIMATE_ONLY hint
* An ability to modify a logical schema without affecting a physical schema by using the LOGICAL_ONLY hint in DDL commands

#### Fixes
* Corrected CHAR and UUID logical datatypes names and length in INFORMATION_SCHEMA
* Fixed MPP-R using LIMIT
* Fixed an MPP-R enriched query for ADQM
* Fixed ADQM LL-R JOIN
* Fixed CONFIG_STORAGE_ADD
* Patched JDBC getDate, getTime, getTimestamp to use a specified calendar
* Fixed CONFIG_STORAGE_ADD
* Fixed INFORMATION_SCHEMA.TABLES displaying a logical table datasource after dropping the logical table with the said datasource
* Fixed the recognition of the select category "Undefined type"

#### Changes
* CHECK_SUM and CHECK_DATA can use a normalization parameter for extra-large delta uploads
* Changed the CHECK_SUM and CHECK_DATA summation algorithm
* Included a commons-lang library into JDBC
* Enabled a STACKTRACE logging for blocked threads
* CREATE TABLE in ADQM, ADG, ADP no longer bypasses a check for a sharding key being a subset of a PK
* Changed the CHECK_DATA and the CHECK_SUM parsing to return more specific error messages
* Updated some error messages to be more informational


### Prostore 5.0.0, 2021-08-12

#### New functionality
* New datasource type ADP (PostgreSQL datasource)
	* Prerequisites: https://github.com/arenadata/kafka-postgres-connector
* Enabled MPP-R source specification within a respective query
* Added a new column TABLE_DATASOURCE_TYPE to INFORMATION_SCHEMA.TABLES
* Implemented subqueries in a SELECT clause: SELECT * FROM tbl1 WHERE tbl1.id IN (subquery)
#### Fixes

* Enabled more than 20 parameters for the IN keyword

#### Changes
* Enriched ADG error message logs (timeouts etc.)
* Changed names of the INFORMATION_SCHEMA sharding keys (TABLE_CONSTRAINTS, KEY_COLUMN_USAGE) to include a datamart name
* Refactored query enrichment for all of the Plugins (ADB, ADG, ADQM) to support the subqueries (see above)
* Fully reworked ADQM enrichment implementation
* Made component names of CHECK_VERSIONS to be consistent with product names


### Prostore 4.1.0, 2021-07-26

#### Changes

* ROLLBACK DELTA stops running MPP-W operations  
* Refactored the stopping mechanizm of MPP-W operations
* SQL+ DML SELECT valid syntax extended for GROUP BY, ORDER BY, LIMIT, OFFSET keywords combination
* Added support for functions within JOIN condition

### Prostore 4.0.1, 2021-07-20

Fixed start for a configuration without ADG.

### Prostore 4.0.0, 2021-07-12

#### New functionality

* New logical entity type - automatically synchronized logical materialized view \(ADB -> ADG\)

    * Prerequisites: [tarantool-pxf-connector version 1.0](https://github.com/arenadata/tarantool-pxf-connector/releases/tag/v1.0) and [adg version 0.3.5](https://github.com/arenadata/kafka-tarantool-loader/releases/tag/0.3.5) or higher
    * See the documentation for full information
    

#### Fixes

* Found and eliminated the intermittent ADB MPP-W failure “FDW server already exists”
* JDBC resultset meta-data patched for LINK, UUID logical types
* Patched LL-R query with `count` and `limit` or `fetch next N rows only`
* Patched ADQM MPP-W for the tables with PK contains Int32 type 

#### Changes

* An upgraded Vert.X 4.1, SQL client included
* Enriched logs with the unique operation identifier. The logging template parameter is `%vcl{requestId:-no_id}`
* Changed ADB LL-R queries to use Statement instead of PreparedStatement when no parameters supplied
* Added the explicit restriction for duplicate column names for logical entities
* Added a check of a timestamp string format on parsing stage for views and materialized views
* MPP-W failures log info extended
* Updated some error messages to be more informational 


### Prostore 3.7.3, 2021-06-30
#### Performance optimization
* Optimized ADB sql client connection parameters to maximize requests throughput.
* JDBC logging is off by default.
* Query-execution-core new configuration parameters:
    * `executorsCount`: $\{ADB\_EXECUTORS\_COUNT:20\}
    * `poolSize`: $\{ADB\_MAX\_POOL\_SIZE:5\}
    * `worker-pool`: $\{DTM\_CORE\_WORKER\_POOL\_SIZE:20\}
    * `event-loop-pool`: $\{DTM\_CORE\_EVENT\_LOOP\_POOL\_SIZE:20\}
* Removed Query-execution-core configuration parameter:
    * `maxSize`: $\{ADB\_MAX\_POOL\_SIZE:5\}
