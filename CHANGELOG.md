# Prostore 4.1.0, 2021-07-26

### Changes

* ROLLBACK DELTA stops running MPP-W operations  
* Refactored the stopping mechanizm of MPP-W operations
* SQL+ DML SELECT valid syntax extended for GROUP BY, ORDER BY, LIMIT, OFFSET keywords combination
* Added support for functions within JOIN condition

# Prostore 4.0.1, 2021-07-20

Fixed start for a configuration without ADG.

# Prostore 4.0.0, 2021-07-12

### New functionality

* New logical entity type - automatically synchronized logical materialized view \(ADB -> ADG\)

    * Prerequisites: [tarantool-pxf-connector version 1.0](https://github.com/arenadata/tarantool-pxf-connector/releases/tag/v1.0) and [adg version 0.3.5](https://github.com/arenadata/kafka-tarantool-loader/releases/tag/0.3.5) or higher
    * See the documentation for full information
    

### Fixes

* Found and eliminated the intermittent ADB MPP-W failure “FDW server already exists”
* JDBC resultset meta-data patched for LINK, UUID logical types
* Patched LL-R query with `count` and `limit` or `fetch next N rows only`
* Patched ADQM MPP-W for the tables with PK contains Int32 type 

### Changes

* An upgraded Vert.X 4.1, SQL client included
* Enriched logs with the unique operation identifier. The logging template parameter is `%vcl{requestId:-no_id}`
* Changed ADB LL-R queries to use Statement instead of PreparedStatement when no parameters supplied
* Added the explicit restriction for duplicate column names for logical entities
* Added a check of a timestamp string format on parsing stage for views and materialized views
* MPP-W failures log info extended
* Updated some error messages to be more informational 

---
# Prostore 3.7.3, 2021-06-30
### Performance optimization
* Optimized ADB sql client connection parameters to maximize requests throughput.
* JDBC logging is off by default.
* Query-execution-core new configuration parameters:
    * `executorsCount`: $\{ADB\_EXECUTORS\_COUNT:20\}
    * `poolSize`: $\{ADB\_MAX\_POOL\_SIZE:5\}
    * `worker-pool`: $\{DTM\_CORE\_WORKER\_POOL\_SIZE:20\}
    * `event-loop-pool`: $\{DTM\_CORE\_EVENT\_LOOP\_POOL\_SIZE:20\}
* Removed Query-execution-core configuration parameter:
    * `maxSize`: $\{ADB\_MAX\_POOL\_SIZE:5\}
