## Configuration
Please see the **etc** directory for sample configuration files
### Mandatory parameters

`a2.jdbc.url` - JDBC connection URL. The following URL formats are supported:
1. EZConnect Format

```
jdbc:oracle:thin:@[[protocol:]//]host1[,host2,host3][:port1][,host4:port2] [/service_name][:server_mode][/instance_name][?connection properties]
```

2. TNS URL Format

```
jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=<protocol>) (HOST=<dbhost>)(PORT=<dbport>)) (CONNECT_DATA=(SERVICE_NAME=<service-name>))
```

3. TNS Alias Format

```
jdbc:oracle:thin:@<alias_name>
```
For more information and examples for JDBC URL format please see [Oracle® Database JDBC Java API Reference, Release 23c](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/)

`a2.wallet.location` - Location of Oracle Wallet/[External Password Store](https://docs.oracle.com/en/database/oracle/oracle-database/21/dbseg/configuring-authentication.html#GUID-2419D309-5874-4FDC-ADB7-65D5983B2053). Not required when `a2.jdbc.url` & `a2.jdbc.username` & `a2.jdbc.password` are set

`a2.jdbc.username` - JDBC connection username. Not required when using Oracle Wallet/[External Password Store](https://docs.oracle.com/en/database/oracle/oracle-database/21/dbseg/configuring-authentication.html#GUID-2419D309-5874-4FDC-ADB7-65D5983B2053) i.e. when `a2.wallet.location` set to proper value

`a2.jdbc.password` - JDBC connection password. Not required when using Oracle Wallet[External Password Store](https://docs.oracle.com/en/database/oracle/oracle-database/21/dbseg/configuring-authentication.html#GUID-2419D309-5874-4FDC-ADB7-65D5983B2053) i.e. when `a2.wallet.location` set to proper value


### Optional parameters

`a2.schema.type` - _Source Connector_ only: default _kafka_. This parameter tells **oracdc** which schema use, and which key & value converters use.
When set to _kafka_ **oracdc**  produces separate schemas for key and value fields in message.
When set to _single_ **oracdc**  produces single schema for all fields.
When set to _debezium_  **oracdc** produces [Debezium](https://debezium.io/documentation/reference/0.10/configuration/avro.html) like messages. Messages in this mode can be consumed with internal **oracdc** sink connector. 

`a2.topic.prefix` - _Source Connector_ only: default _<EMPTYSTRING>_ prefix to prepend table names to generate name of Kafka topic. This parameter is used when **oracdc** configured with `a2.schema.type`=_kafka_ 

`a2.kafka.topic` - _Source Connector_ only: topic to send data, default _oracdc-topic_ . This parameter is used when **oracdc** configured with `a2.schema.type`=_debezium_ 

`a2.topic.partition` - Kafka [topic partition](https://kafka.apache.org/documentation/#intro_concepts_and_terms) to write data. Default - 0.

`a2.batch.size` - default _1000_, maximum number of rows to include in a single batch when polling for new data in _Source Connector_ or  consuming in _Sink Connector_

`a2.poll.interval` - _Source Connector_ only: interval in milliseconds to poll for new data in each materialized view log, default _1000_

`a2.exclude` - _Source Connector_ only: comma separated list of table names or table names with schema name (**<SCHEMA_NAME>.<TABLE_NAME>**) to exclude from **oracdc** processing. To exclude all schema objects from **oracdc** processing use __<SCHEMA_NAME>.*__ or __<SCHEMA_NAME>.%__

`a2.include` - _Source Connector_ only: comma separated list of table names or table names with schema name (**<SCHEMA_NAME>.<TABLE_NAME>**) to include to **oracdc** processing. To include all schema objects to **oracdc** processing use __<SCHEMA_NAME>.*__ or __<SCHEMA_NAME>.%__

`a2.autocreate`- _Sink Connector_ only: default _false_, when set to true **oracdc** creates missing table automatically

`a2.protobuf.schema.naming` - _Source Connector_ only: Default - false. When set to true oracdc generates schema names as valid Protocol Buffers identifiers using underscore as separator. When set to false (default) oracdc generates schema names using dot as separator.

### solutions.a2.cdc.oracle.OraCdcLogMinerConnector specific parameters

`a2.first.change` - When set DBMS_LOGMNR.START_LOGMNR will start mining from this SCN. When not set **min(FIRST_CHANGE#) from V$ARCHIVED_LOG** will used. Overrides SCN value  stored in offset file.

`a2.tmpdir` - Temporary directory for off-heap storage. Default - value of _java.io.tmpdir_ JVM property

`a2.oracdc.schemas` - Use oracdc schemas (**solutions.a2.cdc.oracle.data.OraNumber** and **solutions.a2.cdc.oracle.data.OraTimestamp**) for Oracle datatypes (NUMBER, TIMESTAMP WITH [LOCAL] TIMEZONE). Default false.

`a2.dictionary.file` - File with stored columns data type mapping. For more details contact us at oracle@a2-solutions.eu. This file can be prepared using Schema Editor GUI (solutions.a2.cdc.oracle.schema.TableSchemaEditor)

`a2.initial.load` - A mode for performing initial load of data from tables when set to `EXECUTE`. Record the successful completion of the initial load in the offset file. Default value - `IGNORE`. 

`a2.topic.name.style` - Kafka topic naming convention when `a2.schema.type=kafka`. Valid values - `TABLE` (default), `SCHEMA_TABLE`, `PDB_SCHEMA_TABLE`. 

`a2.topic.name.delimiter` - Kafka topic name delimiter when `a2.schema.type=kafka` and `a2.topic.name.style` set to `SCHEMA_TABLE` or `PDB_SCHEMA_TABLE`. Valid values - `_` (default), `-`, and `.`. 

`a2.table.list.style` - When set to `static` (default) **oracdc** reads tables and partition list to process only at startup according to values of `a2.include` and `a2.exclude` parameters. When set to `dynamic` **oracdc** builds list of objects to process on the fly

`a2.process.lobs` - process Oracle BLOB, CLOB, NCLOB, and [XMLType](https://docs.oracle.com/en/database/oracle/oracle-database/23/adxdb/intro-to-XML-DB.html#GUID-02592188-AC38-4D00-A2FD-9E53604065C8) columns. Default - _false_

`a2.lob.transformation.class` - name of class which implements _solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf_ interface. Default - _solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl_ which just passes information about and values of BLOB/CLOB/NCLOB/XMLTYPE columns to Kafka Connect without performing any additional transformation

`a2.connection.backoff` - Backoff time in milliseconds between reconnectoion attempts. Default - _30000ms_

`a2.archived.log.catalog` - name of class which implements _solutions.a2.cdc.oracle.OraLogMiner_ interface. Default - _solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl_ which reads archived log information and information about next available archived redo log from [V$ARCHIVED_LOG](https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-ARCHIVED_LOG.html) fixed view

`a2.fetch.size` - number of rows fetched with each RDBMS round trip for accessing [V$LOGMNR_CONTENTS](https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-LOGMNR_CONTENTS.html) fixed view. Default 32

`a2.logminer.trace` - trace with 'event 10046 level 8' LogMiner calls? Default - false. To enable tracing the following statements are executed at RDBMS session

```
alter session set max_dump_file_size=unlimited;
alter session set tracefile_identifier='oracdc';
alter session set events '10046 trace name context forever, level 8';

```

`a2.use.rac` - When set to *true* **oracdc** first tried to detect is this connection to [Oracle RAC](https://www.oracle.com/database/real-application-clusters/) by querying the fixed table [V$ACTIVE_INSTANCES](https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-ACTIVE_INSTANCES.html). If database is not RAC, only the warning message is printed. If **oracdc** is connected to [Oracle RAC](https://www.oracle.com/database/real-application-clusters/) by querying the fixed table [V$ACTIVE_INSTANCES](https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-ACTIVE_INSTANCES.html) additional checks are performed and **oracdc** starts a separate task for each redo thread/RAC instance. Changes for the same table from different redo threads/RAC instances are delivered to the same topic but to different partition where

```
 <KAFKA_PARTITION_NUMBER> = <THREAD#> - 1
 ```
Default - *false*.

`a2.transaction.implementation` - 	Queue implementation for processing SQL statements within transactions. Allowed values: ``ChronicleQueue`` and ``ArrayList``. Default - ``ChronicleQueue``. LOB processing is only possible if `a2.transaction.implementation` is set to ``ChronicleQueue``.

`a2.print.invalid.hex.value.warning` - When set to *true* **oracdc** prints information about invalid hex values (like single byte value for DATE/TIMESTAMP/TIMESTAMPTZ) in log. Default - *false*.

`a2.process.online.redo.logs` - When set to _true_ **oracdc** process online redo logs. Default - *false*.

`a2.scn.query.interval.ms` - Minimum time in milliseconds to determine the current SCN during online redo log processing. Used when `a2.process.online.redo.logs` is set true. Default - 60_000.

`a2.incomplete.redo.tolerance` - Connector behavior when processing an incomplete redo record. Allowed values are *error*, *skip*, and *restore*. Default - *error*. When set to:
- *error* **oracdc** prints information about incomplete redo record and stops connector.
- *skip* **oracdc** prints information about incomplete redo record and continue processing.
- *restore* **oracdc** tries to restore missed information from actual row incarnation from the table using ROWID from redo the record.

`a2.print.all.online.scn.ranges` - If set to _true_ **oracdc** prints detailed information about SCN ranges when working with the online log every time interval specified by the `a2.scn.query.interval.ms` parameter. If set to _false_ **oracdc** prints information about current online redo only when **SEQUENCE#** is changed.
Default - *true*.

`a2.log.miner.reconnect.ms` - The time interval in milliseconds after which a reconnection to LogMiner occurs, including the re-creation of the Oracle connection. Unix/Linux only, on Windows **oracdc** creates new LogMiner session and re-creation of database connection every time DBMS_LOGMNR.START_LOGMNR is called. Default - [Long.MAX_VALUE](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Long.html#MAX_VALUE)

`a2.pk.type` - When set to _well_defined_ the key fields are the table's [primary key](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-E1033BB9-0F67-4E59-82AC-B8B572FD82BB) columns or, if the table does not have a [primary key](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-E1033BB9-0F67-4E59-82AC-B8B572FD82BB), the table's [unique key](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-077C26A1-49C3-4E72-AE1D-7CEDD997917A) columns in which all columns are [NOT NULL](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-CF2E06A6-6A35-46CE-808E-305A459457CC).  If there are no appropriate keys in the table, **oracdc** uses the `a2.use.rowid.as.key` parameter and generates a pseudo key based on the row's [ROWID](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/ROWID-Pseudocolumn.html), or generates a schema without any key fields.
When set to _any_unique_ and the table does not have a [primary key](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-E1033BB9-0F67-4E59-82AC-B8B572FD82BB) or a [unique key](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-077C26A1-49C3-4E72-AE1D-7CEDD997917A) with all [NOT NULL](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-CF2E06A6-6A35-46CE-808E-305A459457CC) columns, then the key fields will be the [unique key](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-077C26A1-49C3-4E72-AE1D-7CEDD997917A) columns which may have [NULL](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Nulls.html) columns. If there are no appropriate keys in the table, **oracdc** uses the `a2.use.rowid.as.key` parameter and generates a pseudo key based on the row's [ROWID](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/ROWID-Pseudocolumn.html), or generates a schema without any key fields. Default - *well_defined*.

`a2.use.rowid.as.key` - When set to _true_ and the table does not have a appropriate [primary](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-E1033BB9-0F67-4E59-82AC-B8B572FD82BB) or [unique](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-077C26A1-49C3-4E72-AE1D-7CEDD997917A) key, **oracdc** adds surragate key using the [ROWID](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/ROWID-Pseudocolumn.html). When set to _false_ and the table does not have a appropriate [primary](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-E1033BB9-0F67-4E59-82AC-B8B572FD82BB) or [unique](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-077C26A1-49C3-4E72-AE1D-7CEDD997917A) key, **oracdc** generates schema for the table without any key fields and key schema. Default - *true*.

`a2.use.all.columns.on.delete` - When set to _false_ (default) **oracdc** reads and processes only the [primary](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-integrity.html#GUID-E1033BB9-0F67-4E59-82AC-B8B572FD82BB) key columns from the redo record and sends only the key fields to the Apache Kafka topic. When set to _true_ **oracdc** reads and processes all table columns from the redo record. Default - *false*.

`a2.topic.mapper` - The fully-qualified class name of the class that specifies which Kafka topic the data from the tables should be sent to. If value of thee parameter `a2.shema.type` is set to `debezium`, the default OraCdcDefaultTopicNameMapper uses the parameter `a2.kafka.topic` value as the Kafka topic name, otherwise it constructs the topic name according to the values of the parameters `a2.topic.prefix`, `a2.topic.name.style`, and `a2.topic.name.delimiter`, as well as the table name, table owner and PDB name.
Default - *solutions.a2.cdc.oracle.OraCdcDefaultTopicNameMapper*

`a2.stop.on.ora.1284` - If set to true, the connector stops on an Oracle database error 'ORA-01284: file <Absolute-Path-To-Log-File> cannot be opened'. If set to false, the connector prints an error message and continues processing.
Default - *true*.

`a2.print.unable.to.delete.warning` - If set to true, the connector prints a warning message including all redo record details about ignoring the DELETE operation for tables without a primary key or it surrogate or a schema that does not contain key information. If set to false, the connector does not print a warning message about ignoring the DELETE operation.
Default - *false*.

`a2.schema.name.mapper` - The fully-qualified class name of the class that constructs schema name from the Oracle PDB name (if present), the table owner, and the table name.
Default - *solutions.a2.cdc.oracle.OraCdcDefaultSchemaNameMapper* 

`a2.pseudocolumn.ora_rowscn` - The name of the field in the Kafka Connect record that contains the SCN where the row change was made. If the value is empty, the SCN field is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include SCN field in Kafka Connect record.

`a2.pseudocolumn.ora_commitscn` - The name of the field in the Kafka Connect record that contains the commit SCN of the transaction in which the row change was made. If the value is empty, the commit SCN field is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include commit SCN field in Kafka Connect record

`a2.pseudocolumn.ora_rowts` - The name of the field in the Kafka Connect record that contains the database server timestamp where the row change was made. If the value is empty, the timestamp field is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include row change timestamp field in Kafka Connect record

`a2.pseudocolumn.ora_operation` - The name of the field in the Kafka Connect record that contains the name of the operation (UPDATE/INSERT/DELETE) that changed the database row. If the value is empty, the operation field is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include row change operation field in Kafka Connect record

`a2.pseudocolumn.ora_xid` - The name of the field in the Kafka Connect record that contains the XID (transaction Id) of the transaction that changed the database row. If the value is empty, the XID field is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include XID field in Kafka Connect record

`a2.pseudocolumn.ora_username` - The name of the field in the Kafka Connect record that contains the name of the the user who executed the transaction. If the value is empty, the username is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include username field in Kafka Connect record

`a2.pseudocolumn.ora_osusername` - The name of the field in the Kafka Connect record that contains the name of the the OS user who executed the transaction. If the value is empty, the OS username is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include OS username field in Kafka Connect record

`a2.pseudocolumn.ora_hostname` - The name of the field in the Kafka Connect record that contains the hostname of the machine from which the user connected to the database. If the value is empty, the hostname is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include hostname field in Kafka Connect record

`a2.pseudocolumn.ora_audit_session_id` - The name of the field in the Kafka Connect record that contains the audit session ID associated with the user session making the change. If the value is empty, the audit session id field is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include audit session ID field in Kafka Connect record

`a2.pseudocolumn.ora_session_info` - The name of the field in the Kafka Connect record that contains the information about the database session that executed the transaction. If the value is empty, the session info field is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include session info field in Kafka Connect record

`a2.pseudocolumn.ora_client_id` - The name of the field in the Kafka Connect record that contains the client identifier in the session that executed the transaction (if available). If the value is empty, the client identifier field is not included in the Kafka Connect records.
Default - *<EMPTY_STRING>*, i.e. do not include client identifier field in Kafka Connect record

`a2.key.override` - 	A comma separated list of elements in the format TABLE_OWNER.TABLE_NAME=NOKEY|ROWID|INDEX(INDEX_NAME). If there is a table in this list, then the values ​​of the `a2.pk.type` and `a2.use.rowid.as.key` parameters for it are ignored and the values ​​of the key columns are set in accordance with this parameter:
 `NONE` - do not create key fields in the Kafka topic for this table
 `ROWID` - use ROWID as a key field in the Kafka topic with the name ORA_ROW_ID and type STRING
 `INDEX(INDEX_NAME)` use the index columns of index named INDEX_NAME as key fields of the Kafka topic
Default - *<EMPTY_STRING>*


`a2.last.sequence.notifier` - The fully-qualified class name of the class that implements LastProcessedSeqNotifier interface to send notifications about the last processed log sequence. Currently there is only a notifier that writes a last processed sequence number to a file. To configure it, you need to set the value of the parameter `a2.last.sequence.notifier` to `solutions.a2.cdc.oracle.OraCdcLastProcessedSeqFileNotifier` and the value of the parameter `a2.last.sequence.notifier.file` to the name of the file in which the last processed number will be written.

`a2.last.sequence.notifier.file` - The name of the file in which the last processed number will be written.
Default - *${connectorName}.seq*

`a2.transactions.threshold` - Maximum threshold of simultaneously processed (both in the process of reading from the database and in the process of sending) transactions in the connector on Linux systems. When not specified (0, default) value is calculated as (vm.max_map_count/16) * 7 for Linux or default to 0x7000 on other platforms.

`a2.reduce.load.ms` - Wait time in ms to reduce the number of simultaneously processed transactions. Sending of processed messages continues, pause occurs only for the process of reading from the database. Default - 60_000 

`a2.array.list.default.capacity` - Initial capacity of ArrayList storing Oracle Database transaction data. Default - 32.

`a2.number.map.[PDB_NAME.]SCHEMA_NAME.TABLE_NAME.COL_NAME_OR_PATTERN` - Overrides the Kafka data type for an Oracle column with data type NUMBER. The *%* symbol can be used as a control sign in the column name, its use is supported as a start symbol (i.e. the column name ends with) and as a terminal symbol (i.e. the column name begins with). Possible values ​​of data types:
- BOOL, BOOLEAN 
- BYTE, TINYINT
- SHORT, SMALLINT
- INT, INTEGER
- LONG, BIGINT
- FLOAT
- DOUBLE
- DECIMAL([P],S), NUMERIC([P],S)


#### solutions.a2.cdc.oracle.OraCdcLogMinerConnector physical standby connection parameters

`a2.standby.activate` - activate running LogMiner at physical standby database. Default - *false*.

`a2.standby.wallet.location` - Location of Oracle Wallet/[External Password Store](https://docs.oracle.com/en/database/oracle/oracle-database/21/dbseg/configuring-authentication.html#GUID-2419D309-5874-4FDC-ADB7-65D5983B2053) for connecting to physical standby database with [V$DATABASE.OPEN_MODE = MOUNTED](https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-DATABASE.html)

`a2.standby.jdbc.url` - JDBC connection URL to connect to [Physical Standby Database](https://docs.oracle.com/en/database/oracle/oracle-database/21/sbydb/introduction-to-oracle-data-guard-concepts.html#GUID-C49AC6F4-C89B-4487-BC18-428D65865B9A). For information about syntax please see description of parameter `a2.jdbc.url` above


#### solutions.a2.cdc.oracle.OraCdcLogMinerConnector distributed mode parameters

`a2.distributed.activate` - Use **oracdc** in distributed configuration (redo logs are generated at source RDBMS server and then transferred to compatible target RDBMS server for processing with LogMiner. For description of this configuration please look at _Figure 25-1_ at [Using LogMiner to Analyze Redo Log Files](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html). Default - _false_

`a2.distributed.wallet.location` - Location of Oracle Wallet/[External Password Store](https://docs.oracle.com/en/database/oracle/oracle-database/21/dbseg/configuring-authentication.html#GUID-2419D309-5874-4FDC-ADB7-65D5983B2053) for connecting to target database in distributed mode

`a2.distributed.jdbc.url` - JDBC connection URL to connect to [Mining Database](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html#GUID-03892F75-767E-4462-9865-9843F1502AD5). For information about syntax please see description of parameter `a2.jdbc.url` above

`a2.distributed.target.host` - hostname of the target (where dbms_logmnr runs) database on which the shipment agent is running

`a2.distributed.target.port` - port number on which shipping agent listens for requests

### Redo miner (direct reading of redo without LogMiner) specific parameters

`a2.redo.filename.convert` - It converts the filename of a redo log to another path. It is specified as a string in the <ORIGINAL_PATH>=<NEW_PATH> format. If not specified (default), no conversion occurs.

`a2.storage.media` - Parameter defining the storage medium for redo log files: `FS` - redo files will be read from the local file system, `ASM` - redo files will be read from the Oracle ASM, `SSH` - redo files will be read from the remote file system using ssh, `SMB`  - redo files will be read from the remote file system using smb. Default - FS

`a2.asm.jdbc.url` - JDBC URL pointing to the Oracle ASM instance. For information about syntax please see description of parameter 'a2.jdbc.url' above

`a2.asm.username` - Username for connecting to Oracle ASM instance, must have SYSASM role

`a2.asm.password` - User password for connecting to Oracle ASM instance

`a2.asm.read.ahead` - When set to true (the default), the connector reads data from the redo logs in advance, with chunks larger than the redo log file block size.

`a2.asm.reconnect.ms` - The time interval in milleseconds after which a reconnection to Oracle ASM occurs, including the re-creation of the Oracle connection. Default - 604,800,000 ms (one week)

`a2.ssh.hostname` - FQDN or IP address of the remote server with redo log files

`a2.ssh.port` - SSH port of the remote server with redo log files. Default - 22

`a2.ssh.user` - Username for the authentication to the remote server with redo log files

`a2.ssh.private.key` - Private key for the authentication to the remote server with redo log files

`a2.ssh.password` - Password for the authentication to the remote server with redo log files

`a2.ssh.reconnect.ms` - The time interval in milliseconds after which a reconnection to remote server with redo files, including the re-creation of the SSH connection.
Default - 86,400,000 (24 hours)

`a2.ssh.strict.host.key.checking` - SSH strict host key checking. Default - false.

`a2.ssh.provider` - Library that provides SSH connection: maverick for Maverick Synergy (https://jadaptive.com/) or sshj for Hierynomus sshj (https://github.com/hierynomus/sshj). Default - maverick

`a2.ssh.max.unconfirmed.reads` - Maximum number of unconfirmed reads from SFTP server when using Hierynomus sshj. Default - 256

`a2.ssh.buffer.size` - Read-ahead buffer size in bytes for fata from SFTP server when using Hierynomus sshj. Default - 32768

`a2.smb.server` - FQDN or IP address of the remote SMB (Windows) server with redo log files

`a2.smb.share.online` - Name of the SMB (Windows) share with online redo logs

`a2.smb.share.archive` - Name of the SMB (Windows) share with archived redo logs

`a2.smb.user` - Username for the authentication to the remote SMB (Windows) server with redo log files

`a2.smb.password` - Password for the authentication to the remote SMB (Windows) server with redo log files

`a2.smb.domain` - SMB (Windows) authentication domain name

`a2.smb.timeout` - SMB read timeout in ms. Default - 180_000

`a2.smb.socket.timeout` - SMB read timeout in ms. Default - 180_000

`a2.smb.reconnect.ms` - The time interval in milliseconds after which a reconnection to remote server with redo files, including the re-creation of the SMB (Windows) connection.
Default - 86,400,000 (24 hours)

`a2.smb.buffer.size` - Read-ahead buffer size in bytes for fata from SMB (Windows) server. Default - 32768
