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
