# DTM JDBC Driver

JDBC driver for connecting to the core.

### Connection via SQL clients

Driver class ``io.arenadata.dtm.jdbc.DtmDriver``

Connection string : ``jdbc:adtm://{host}:{port}/{datamartMnemonics}``

``host`` - ip address where core is deployed

``port`` - port where core is deployed

``datamartMnemonics`` - name of the connected datamart. Optional attribute
