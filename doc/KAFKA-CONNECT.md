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

### solutions.a2.cdc.oracle.OraCdcLogMinerConnector specific parameters
`a2.redo.count` - Quantity of archived logs to process during each DBMS_LOGMNR.START_LOGMNR call, default _1_

`a2.redo.size` - Minimal size of archived logs to process during each DBMS_LOGMNR.START_LOGMNR call. When set value of `a2.redo.count` is ignored

`a2.first.change` - When set DBMS_LOGMNR.START_LOGMNR will start mining from this SCN. When not set **min(FIRST_CHANGE#) from V$ARCHIVED_LOG** will used. Overrides SCN value  stored in offset file.

`a2.tmpdir` - Temporary directory for off-heap storage. Default - value of _java.io.tmpdir_ JVM property

`a2.persistent.state.file` - Name of file to store oracdc state between restart. Default `$TMPDIR/oracdc.state`

`a2.oracdc.schemas` - Use oracdc schemas (**solutions.a2.cdc.oracle.data.OraNumber** and **solutions.a2.cdc.oracle.data.OraTimestamp**) for Oracle datatypes (NUMBER, TIMESTAMP WITH [LOCAL] TIMEZONE). Default false.

`a2.dictionary.file` - File with stored columns data type mapping. For more details contact us at oracle@a2-solutions.eu. This file can be prepared using Schema Editor GUI (solutions.a2.cdc.oracle.schema.TableSchemaEditor)

`a2.initial.load` - A mode for performing initial load of data from tables when set to `EXECUTE`. Record the successful completion of the initial load in the offset file. Default value - `IGNORE`. 

`a2.topic.name.style` - Kafka topic naming convention when `a2.schema.type=kafka`. Valid values - `TABLE` (default), `SCHEMA_TABLE`, `PDB_SCHEMA_TABLE`. 

`a2.topic.name.delimiter` - Kafka topic name delimiter when `a2.schema.type=kafka` and `a2.topic.name.style` set to `SCHEMA_TABLE` or `PDB_SCHEMA_TABLE`. Valid values - `_` (default), `-`, and `.`. 

`a2.table.list.style` - When set to `static` (default) **oracdc** reads tables and partition list to process only at startup according to values of `a2.include` and `a2.exclude` parameters. When set to `dynamic` **oracdc** builds list of objects to process on the fly

`a2.process.lobs` - process Oracle BLOB, CLOB, NCLOB, and [XMLType](https://docs.oracle.com/en/database/oracle/oracle-database/21/adxdb/intro-to-XML-DB.html#GUID-02592188-AC38-4D00-A2FD-9E53604065C8) columns. Default - _false_

`a2.lob.transformation.class` - name of class which implements _solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf_ interface. Default - _solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl_ which just passes information about and values of BLOB/CLOB/NCLOB/XMLTYPE columns to Kafka Connect without performing any additional transformation

`a2.connection.backoff` - Backoff time in milliseconds between reconnectoion attempts. Default - _30000ms_

`a2.archived.log.catalog` - name of class which implements _solutions.a2.cdc.oracle.OraLogMiner_ interface. Default - _solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl_ which reads archived log information and information about next available archived redo log from [V$ARCHIVED_LOG](https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-ARCHIVED_LOG.html) fixed view

`a2.fetch.size` - number of rows fetched with each RDBMS round trip for accessing [V$LOGMNR_CONTENTS](https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-LOGMNR_CONTENTS.html) fixed view. Default 32

`a2.logminer.trace` - trace with 'event 10046 level 8' LogMiner calls? Default - false. To enable tracing the following statements are executed at RDBMS session

```
alter session set max_dump_file_size=unlimited;
alter session set tracefile_identifier='oracdc';
alter session set events '10046 trace name context forever, level 8';

```

`a2.resiliency.type` - How restarts and crashes are handled: In ``legacy`` mode (the default), all information is stored in the file system, delivery of all changes is guaranteed with exactly-once semantics, but this mode does not protect against file system failures. When set to ``fault-tolerant``, all restart data stored on Kafka topics, the connector depends only on Kafka cluster, but if an error occurs in the middle of sending a Oracle transaction to the Kafka broker, that transaction will be re-read from archived redo and sending to Kafka will continue after last successfully processed record to maintain exactly-once semantics

#### solutions.a2.cdc.oracle.OraCdcLogMinerConnector physical standby connection parameters

`a2.standby.activate` - activate running LogMiner at physical standby database. Default - _false_

`a2.standby.wallet.location` - Location of Oracle Wallet for connecting to physical standby database with [V$DATABASE.OPEN_MODE = MOUNTED](https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-DATABASE.html)

`a2.standby.tns.admin` - Location of tnsnames.ora file for connecting to physical standby database with [V$DATABASE.OPEN_MODE = MOUNTED](https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-DATABASE.html)

`a2.standby.tns.alias` - Connection TNS alias for connecting to physical standby database with [V$DATABASE.OPEN_MODE = MOUNTED](https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-DATABASE.html)

#### solutions.a2.cdc.oracle.OraCdcLogMinerConnector distributed mode parameters

`a2.distributed.activate` - Use **oracdc** in distributed configuration (redo logs are generated at source RDBMS server and then transferred to compatible target RDBMS server for processing with LogMiner. For description of this configuration please look at _Figure 22-1_ at [Using LogMiner to Analyze Redo Log Files](https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/oracle-logminer-utility.html). Default - _false_

`a2.distributed.wallet.location` - Location of Oracle Wallet for connecting to target database in distributed mode

`a2.distributed.tns.admin` - Location of tnsnames.ora file for connecting to target database in distributed mode

`a2.distributed.tns.alias` - Connection TNS alias for connecting to target database in distributed mode

`a2.distributed.target.host` - hostname of the target (where dbms_logmnr runs) database on which the shipment agent is running

`a2.distributed.target.port` - port number on which shipping agent listens for requests
