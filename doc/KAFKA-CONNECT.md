## Configuration
Please see the **etc** directory for sample configuration files
### Mandatory parameters

`a2.jdbc.url` - JDBC connection URL. Not required when using Oracle Wallet

`a2.jdbc.username` - JDBC connection username. Not required when using Oracle Wallet

`a2.jdbc.password` - JDBC connection password. Not required when using Oracle Wallet

`a2.wallet.location` - Location of Oracle Wallet. Not required when `a2.jdbc.url` & `a2.jdbc.username` & `a2.jdbc.password` are set

`a2.tns.admin` - Location of tnsnames.ora file. Not required when `a2.jdbc.url` & `a2.jdbc.username` & `a2.jdbc.password` are set

`a2.tns.alias` - Connection TNS alias. Not required when `a2.jdbc.url` & `a2.jdbc.username` & `a2.jdbc.password` are set

`a2.schema.type` - _Source Connector_ only: default _kafka_. This parameter tells **oracdc** which schema use, and which key & value converters use.
When set to _kafka_ **oracdc**  produces Kafka Connect JDBC connector compatible messages [Confluent JDBC Sink Connector](https://docs.confluent.io/3.2.0/connect/connect-jdbc/docs/sink_connector.html).
When set to _debezium_  **oracdc** produces [Debezium](https://debezium.io/documentation/reference/0.10/configuration/avro.html) like messages. Messages in this mode can be consumed with internal **oracdc** sink connector. 

`a2.topic.prefix` - _Source Connector_ only: default _<EMPTYSTRING>_ prefix to prepend table names to generate name of Kafka topic. This parameter is used when **oracdc** configured with `a2.schema.type`=_kafka_ 

`a2.kafka.topic` - _Source Connector_ only: default _oracdc-topic_ topic to send data. This parameter is used when **oracdc** configured with `a2.schema.type`=_debezium_ 


### Optional parameters

`a2.batch.size` - default _1000_, maximum number of rows to include in a single batch when polling for new data in _Source Connector_ or  consuming in _Sink Connector_

`a2.poll.interval` - _Source Connector_ only: interval in milliseconds to poll for new data in each materialized view log, default _1000_

`a2.exclude` - _Source Connector_ only: comma separated list of table names or table names with schema name (**<SCHEMA_NAME>.<TABLE_NAME>**) to exclude from **oracdc** processing. To exclude all schema objects from **oracdc** processing use __<SCHEMA_NAME>.*__ or __<SCHEMA_NAME>.%__

`a2.include` - _Source Connector_ only: comma separated list of table names or table names with schema name (**<SCHEMA_NAME>.<TABLE_NAME>**) to include to **oracdc** processing. To include all schema objects to **oracdc** processing use __<SCHEMA_NAME>.*__ or __<SCHEMA_NAME>.%__

`a2.autocreate`- _Sink Connector_ only: default _false_, when set to true **oracdc** creates missing table automatically

### eu.solutions.a2.cdc.oracle.OraCdcLogMinerConnector specific parameters
`a2.redo.count` - Quantity of archived logs to process during each DBMS_LOGMNR.START_LOGMNR call, default _1_

`a2.redo.size` - Minimal size of archived logs to process during each DBMS_LOGMNR.START_LOGMNR call. When set value of `a2.redo.count` is ignored

`a2.first.change` - When set DBMS_LOGMNR.START_LOGMNR will start mining from this SCN. When not set **min(FIRST_CHANGE#) from V$ARCHIVED_LOG** will used. Overrides SCN value  stored in offset file.

`a2.tmpdir` - Temporary directory for off-heap storage. Default - value of _java.io.tmpdir_ JVM property

`a2.persistent.state.file` - Name of file to store oracdc state between restart. Default `$TMPDIR/oracdc.state`

`a2.oracdc.schemas` - Use oracdc schemas (**eu.solutions.a2.cdc.oracle.data.OraNumber** and **eu.solutions.a2.cdc.oracle.data.OraTimestamp**) for Oracle datatypes (NUMBER, TIMESTAMP WITH [LOCAL] TIMEZONE). Default false.

#### eu.solutions.a2.cdc.oracle.OraCdcLogMinerConnector physical standby connection parameters

`a2.standby.activate` - activate running LogMiner at physical standby database. Default - _false_

`a2.standby.wallet.location` - Location of Oracle Wallet for connecting to physical standby database with V$DATABASE.OPEN_MODE = MOUNTED

`a2.standby.tns.admin` - Location of tnsnames.ora file for connecting to physical standby database with V$DATABASE.OPEN_MODE = MOUNTED

`a2.standby.tns.alias` - Connection TNS alias for connecting to physical standby database with V$DATABASE.OPEN_MODE = MOUNTED
