# oracdc

**oracdc** is a software package for near real-time data integration and replication in heterogeneous IT environments. **oracdc** consist of two  [Apache Kafka](http://kafka.apache.org/) Source Connector's and JDBC sink connector. **oracdc** provides data integration, [transactional change data capture](https://en.wikipedia.org/wiki/Change_data_capture), and data replication between operational and analytical IT systems. 
Starting from Oracle RDBMS 12c various Oracle tools for [CDC](https://en.wikipedia.org/wiki/Change_data_capture) and/or replication are [deprecated and desupported](https://docs.oracle.com/database/121/UPGRD/deprecated.htm) and replaced by [Oracle Golden Gate](https://www.oracle.com/middleware/technologies/goldengate.html). This project is not intended to be 100% replacement of [expensive](https://www.oracle.com/assets/technology-price-list-070617.pdf) Oracle Golden Gate licenses however may help in many practical cases. Project was tested using [Oracle E-Business Suite](https://www.oracle.com/applications/ebusiness/) customer instance for transferring various information (mostly INV, ONT, WSH, GL & XLA tables) to further reporting and analytics in [PostgreSQL](https://www.postgresql.org/) database. We tested both short transactions (human data entry) and long transactions (various accounting programs) changing millions of rows in dozens of tables (description of some tables used: [WSH_NEW_DELIVERIES](https://docs.oracle.com/cd/E51367_01/scmop_gs/OEDSC/WSH_NEW_DELIVERIES_tbl.htm), [WSH_DELIVERY_ASSIGNMENTS](https://docs.oracle.com/cd/E51367_01/scmop_gs/OEDSC/WSH_DELIVERY_ASSIGNMENTS_tbl.htm), [WSH_DELIVERY_DETAILS](https://docs.oracle.com/cd/E51367_01/scmop_gs/OEDSC/WSH_DELIVERY_DETAILS_tbl.htm)).
**oracdc** Source Connector's compatible with [Oracle RDBMS](https://www.oracle.com/database/index.html) versions 10g, 11g, 12c, 18c, 19c, and 21c. If you need support for Oracle Database 9i and please send us an email at [oracle@a2-solutions.eu](mailto:oracle@a2-solutions.eu).

## eu.solutions.a2.cdc.oracle.OraCdcLogMinerConnector
This Source Connector uses [Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/oracle-logminer-utility.html) as source for data changes. Connector is designed to minimize the side effects of using Oracle LogMiner, even for Oracle RDBMS versions with **DBMS_LOGMNR.CONTINUOUS_MINE** feature support **oracdc** does not use it. Instead, **oracdc** reads **V$LOGMNR_CONTENTS** and saves information with **V$LOGMNR_CONTENTS.OPERATION in ('INSERT', 'DELETE', 'UPDATE')** in Java off-heap memory structures provided by [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue). This approach minimizes the load on the Oracle database server, but requires additional disk space on the server with **oracdc** installed. When restarting, all in progress transactions, records, and objects are saved, their status is written to the file defined by parameter `a2.persistent.state.file` . An example `oracdc.state` file is located in `etc` directory.
**oracdc**'s _eu.solutions.a2.cdc.oracle.OraCdcLogMinerConnector_ connects to the following configurations of Oracle RDBMS:
1. Standalone instance, or Primary Database of Oracle DataGuard Cluster/Oracle Active DataGuard Cluster, i.e. **V$DATABASE.OPEN_MODE = READ WRITE** 
2. Physical Standby Database of Oracle **Active DataGuard** cluster, i.e. **V$DATABASE.OPEN_MODE = READ ONLY**
3. Physical Standby Database of Oracle **DataGuard** cluster, i.e. **V$DATABASE.OPEN_MODE = MOUNTED**. In this mode, a physical standby database is used to retrieve data using LogMiner and connection to primary database is used to perform strictly limited number of queries to data dictionary (ALL|CDB_OBJECTS, ALL|CDB_TABLES, and ALL|CDB_TAB_COLUMNS). This option allows you to promote a physical standby database to source of replication, eliminates LogMiner overhead from primary database, and  decreases TCO of Oracle Database.
4. Running in distributed configuration when source database generates redo log files and also contain dictionary and target database is compatible mining database (see Figure 22-1 in [Using LogMiner to Analyze Redo Log Files](https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/oracle-logminer-utility.html)). **N.B.** Currently only non-CDB distributed database configuration has tested, tests for CDB distributed database configuration are in progress now.

### Monitoring
_eu.solutions.a2.cdc.oracle.OraCdcLogMinerConnector_ publishes a number of metrics about the connector’s activities that can be monitored through JMX. For complete list of metrics please refer to [LOGMINER-METRICS.md](doc/LOGMINER-METRICS.md)

### Oracle Database SecureFiles and Large Objects including SYS.XMLTYPE
Oracle Database SecureFiles and Large Objects i.e. LOB's are supported from v0.9.7 when parameter `a2.process.lobs` set to true. CLOB type supported only for columns with **DBA_LOBS.FORMAT='ENDIAN NEUTRAL'**. If you need support for CLOB columns with **DBA_LOBS.FORMAT='ENDIAN SPECIFIC'** or **XMLTYPE** please send us an email at [oracle@a2-solutions.eu](mailto:oracle@a2-solutions.eu).
SYS.XMLTYPE data are supported from v0.9.8.2 when parameter `a2.process.lobs` set to true.
For processing LOB's and SYS.XMLTYPE please do not forget to set Apache Kafka parameters according to size of LOB's:
1. For Source connector: _producer.max.request.size_
2. For broker: _replica.fetch.max.bytes_ and _message.max.bytes_
By default CLOB, NCLOB, and SYS.XMLTYPE data are compressed using [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression algorithm.

#### Large objects (BLOB/CLOB/NCLOB/XMLTYPE) transformation feature (oracdc 0.9.8.3+)
Apache Kafka is not isn't meant to handle large messages with size over 1MB. But Oracle RDBMS often is used as storage for unstructured information too. For breaking this barrier we designed LOB transformation feature.
Imagine that you have a table with following structure

```
descibe APPLSYS.FND_LOBS
Name                                      Null?    Type
 ----------------------------------------- -------- ----------------
 FILE_ID                                   NOT NULL NUMBER
 FILE_NAME                                          VARCHAR2(256)
 FILE_CONTENT_TYPE                         NOT NULL VARCHAR2(256)
 FILE_DATA                                          BLOB
 UPLOAD_DATE                                        DATE
 EXPIRATION_DATE                                    DATE
 PROGRAM_NAME                                       VARCHAR2(32)
 PROGRAM_TAG                                        VARCHAR2(32)
 LANGUAGE                                           VARCHAR2(4)
 ORACLE_CHARSET                                     VARCHAR2(30)
 FILE_FORMAT                               NOT NULL VARCHAR2(10)
```
and you need the data in this table including BLOB column `FILE_DATA` in the reporting subsystem, which is implemented in different database management system with limited large object support like [Snowflake](https://docs.snowflake.com/en/sql-reference/data-types-unsupported.html) and you have very strict constraints for traffic through Apache Kafka brokers. The best way to solve the problem is in [Reddite quae sunt Caesaris Caesari et quae sunt Dei Deo](https://en.wikipedia.org/wiki/Render_unto_Caesar) where RDBMS will perform all data manipulation and [object storage](https://en.wikipedia.org/wiki/Object_storage) will be used for storing large objects instead of storing it in  BLOB column `FILE_DATA`. To achieve this:
1. Create transformation implementation class

```
package com.example.oracdc;

import eu.solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
// more imports required

public class TransformScannedDataInBlobs implements OraCdcLobTransformationsIntf {

	@Override
	public Schema transformSchema(String pdbName, String tableOwner, String tableName, OraColumn lobColumn,
			SchemaBuilder valueSchema) {
		if ("FND_LOBS".equals(tableName) &&
				"FILE_DATA".equals(lobColumn.getColumnName())) {
			final SchemaBuilder transformedSchemaBuilder = SchemaBuilder
					.struct()
					.optional()
					.name(lobColumn.getColumnName())
					.version(1);
			transformedSchemaBuilder.field("S3_URL", Schema.OPTIONAL_STRING_SCHEMA);
			valueSchema.field(lobColumn.getColumnName(), transformedSchemaBuilder.build());
			return transformedSchemaBuilder.build();
		} else {
			return OraCdcLobTransformationsIntf.super.transformSchema(
					pdbName, tableOwner, tableName, lobColumn,valueSchema);
		}
	}

	@Override
	public Struct transformData(String pdbName, String tableOwner, String tableName, OraColumn lobColumn, byte[] content, Struct keyStruct, Schema valueSchema) {
		if ("FND_LOBS".equals(tableName) &&
				"FILE_DATA".equals(lobColumn.getColumnName())) {
				final Struct valueStruct = new Struct(valueSchema);
				// ...
				final String s3ObjectKey = <SOME_INIQUE_S3_OBJECT_KEY_USING_KEY_STRUCT>
				// ...
				final S3Client s3Client = S3Client
										.builder()
										.region(<VALID_AWS_REGION>)
										.build();
				final PutObjectRequest por = PutObjectRequest
										.builder()
										.bucket(<VALID_S3_BACKET>)
										.key(s3ObjectKey)
										.build();
				s3Client.putObject(por, RequestBody.fromBytes(content));
				// ...
				
				valueStruct.put("S3_URL", s3ObjectKey);
				return valueStruct;
		}
		return null;
	}
}

```
2. Set required ***oracdc*** parameters

```
a2.process.lobs=true
a2.lob.transformation.class=com.example.oracdc.TransformScannedDataInBlobs

```


#### JSON Datatype support for Oracle RDBMS 21c +
Unfortunately, for operations on the JSON data type, the result is returned as (below is results for transaction which includes JSON datatype)

```
select SQL_REDO from V$LOGMNR_CONTENTS where XID='01000E0078020000';

SQL_REDO
--------------------------------------------------------------------------------
set transaction read write
Unsupported
Unsupported
Unsupported
Unsupported
commit

6 rows selected.

```
Wherein

```
alter system dump logfile '/path-to-archived-log-file' scn min XXXXXXXXX scn max YYYYYYYYY;
```
shows correct values in redo block.
Unfortunately, JSON data type is not contained in LogMiner's [Supported Data Types and Table Storage Attributes](https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/oracle-logminer-utility.html#GUID-BA995486-041E-4C83-83EA-D7BC2A866DE3), nor in the [Unsupported Data Types and Table Storage Attributes](https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/oracle-logminer-utility.html#GUID-8A4F98EC-C233-4471-BFF9-9FB35EF5AD0D).
We are watching the status of this issue.


### DDL Support and schema evolution
[Data Definition Language (DDL)](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Types-of-SQL-Statements.html#GUID-FD9A8CB4-6B9A-44E5-B114-EFB8DA76FC88) is currently not supported. Its support is planned for version 0.9.8 (DEC-2020 - JAN-2021) along with support for the [Schema Evolution](https://docs.confluent.io/current/schema-registry/avro.html#schema-evolution) at Apache Kafka side.

### RDBMS errors resiliency and connection retry back-off
**oracdc** resilent to Oracle database shutdown and/or restart while performing [DBMS_LOGMNR.ADD_LOGFILE](https://docs.oracle.com/en/database/oracle/oracle-database/21/arpls/DBMS_LOGMNR.html#GUID-30C5D959-C0A0-4591-9FBA-F57BC72BBE2F) call or waiting for new archived redo log. ORA-17410 ("No more data read from socket") is intercepted and an attempt to reconnect is made after fixed backoff time specified by parameter `a2.connection.backoff`

### Known issues
1. **INTERVAL** data types family not supported yet

### Performance tips
1. If you do not use archivelogs as a source of database user activity audit information, consider setting Oracle RDBMS hidden parameter **_transaction_auditing** to **false** after consulting a [Oracle Support Services](https://www.oracle.com/support/)
2. Always try to set up **supplemental logging** at the table level, and not for all database objects
3. Proper file system parameters and sizing for path where [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) objects resides.
4. Proper open files hard and soft limits for OS user running **oracdc**
5. To determine source of bottleneck set parameter `a2.logminer.trace` to true and analyze waits at Oracle RDBMS side using data from trace file (**tracefile_identifier='oracdc'**)
6. For optimizing network transfer consider increase SDU (Ref.: [Database Net Services Administrator's Guide, Chapter 14 "Optimizing Performance"](https://docs.oracle.com/en/database/oracle/oracle-database/21/netag/optimizing-performance.html)). Also review Oracle Support Services Note 2652240.1[SDU Ignored By The JDBC Thin Client Connection](https://support.oracle.com/epmos/faces/DocumentDisplay?id=2652240.1).
Example listener.ora with SDU set:

```
LISTENER =
(DESCRIPTION_LIST =
  (DESCRIPTION =
    (SDU = 2097152)
    (ADDRESS = (PROTOCOL = IPC)(KEY = EXTPROC1))
    (ADDRESS = (PROTOCOL = TCP)(HOST = 0.0.0.0)(PORT = 1521))
  )
)
```
Example jdbc connect string with SDU set:

```
jdbc:oracle:thin:@(description=(sdu=2097152)(address=(protocol=tcp)(host=oratest01)(port=1521))(connect_data=(server=dedicated)(service_name=KAFKA)))
```
While setting SDU always check live settings using listener trace set to **admin** level. For example:

```
cd <LISTENER_TRACE_DIR>
lsnrctl set trc_level admin
grep nsconneg `lsnrctl show trc_file | grep "set to" | awk {'print $6'}`
```

7. Use **oracdc** JMX performance [metrics]((doc/LOGMINER-METRICS.md)
8. Depending on structure of your data try increasing value of `a2.fetch.size` parameter (default fetch size - 32 rows)
9. Although some documents recommend changing setting of _log_read_buffers & _log_read_buffer_size hidden parameters we didn't see serious improvement or degradation using different combinations of these parameters. For more information please read [LogMiner Tuning: _log_read_buffers & _log_read_buffer_size](https://github.com/averemee-si/oracdc/wiki/LogMiner-Tuning:-_log_read_buffers-&-_log_read_buffer_size)


## eu.solutions.a2.cdc.oracle.OraCdcSourceConnector
This Source Connector uses Oracle RDBMS [materialized view log's](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-MATERIALIZED-VIEW-LOG.html) as source for data changes and materializes Oracle RDBMS materialized view log at heterogeneous database system. No materialized view should consume information from materialized view log's which are used by **oracdc**. Unlike _eu.solutions.a2.cdc.oracle.OraCdcLogMinerConnector_ this SourceConnector works with BLOB, and CLOB data types. If you need support for Oracle Database _LONG_, and/or _LONG RAW_ data types please send us an email at [oracle@a2-solutions.eu](mailto:oracle@a2-solutions.eu).


# Getting Started

These instructions will get you a copy of the project up and running on any platform with JDK8+ support.

## Prerequisites

Before using **oracdc** please check that required Java8+ is installed with

```
echo "Checking Java version"
java -version
```

## Installing

Build with

```
mvn install
```

### Oracle JDBC drivers

**oracdc** is shipped with Oracle JDBC 21.1.0.0, or you can copy drivers from Oracle RDBMS server

```
cp $ORACLE_HOME/jdbc/lib/ojdbc8.jar <JDBC directory> 
cp $ORACLE_HOME/ucp/lib/ucp.jar <JDBC directory> 
cp $ORACLE_HOME/jlib/oraclepki.jar <JDBC directory>
cp $ORACLE_HOME/jlib/osdt_core.jar <JDBC directory>
cp $ORACLE_HOME/jlib/osdt_cert.jar <JDBC directory>
```
or download drivers `ojdbc[JAVA-VERSION].jar`, `ucp.jar`, `oraclepki.jar`, `osdt_core.jar`, and `osdt_cert.jar` from [JDBC and UCP Downloads page](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)

### Oracle Wallet

Please refer to [Oracle Database documentation](https://docs.oracle.com/en/database/) for instructions how to create and manage the Oracle Wallet and tnsnames.ora file. Oracle recommends that you create and manage the Wallet in a database environment using `mkstore` command

```
mkstore -wrl $A2_CDC_HOME/wallets/ -create
mkstore -wrl $A2_CDC_HOME/wallets/ -createCredential R1229 SCOTT
```

# Running 

## Oracle LogMiner as CDC source (eu.solutions.a2.cdc.oracle.OraCdcLogMinerConnector)
The following steps need to be performed in order to prepare the Oracle database so the **oracdc** Connector can be used.
### Enabling Oracle RDBMS ARCHIVELOG mode
Log in to SQL*Plus as SYSDBA and check results of query
 
 ```
 select LOG_MODE from V$DATABASE
 ```
If the query returns **ARCHIVELOG**, it is enabled. Skip ahead to **Enabling supplemental log data**.
If the query returns **NOARCHIVELOG** :

```
shutdown immediate
startup mount
alter database archivelog;
alter database open;
```
To verify that **ARCHIVELOG** has been enabled run again
 
 ```
 select LOG_MODE from V$DATABASE
 ```
This time it should return **ARCHIVELOG**

### Enabling supplemental logging
Log in to SQL*Plus as SYSDBA, if you like to enable supplemental logging for whole database:

```
alter database add supplemental log data (ALL) columns;
```
Alternatively, to enable only for selected tables and minimal supplemental logging, a database-level option (recommended):

```
alter database add supplemental log data;
alter table <OWNER>.<TABLE_NAME> add supplemental log data (ALL) columns; 
```
If using Amazon RDS for Oracle please see [AWS Amazon Relational Database Service User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.Oracle.CommonDBATasks.Log.html#Appendix.Oracle.CommonDBATasks.AddingSupplementalLogging) about **rdsadmin.rdsadmin_util.alter_supplemental_logging** procedure.

To verify **supplemental logging** settings at database level:

```
select SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK, SUPPLEMENTAL_LOG_DATA_UI, SUPPLEMENTAL_LOG_DATA_FK, SUPPLEMENTAL_LOG_DATA_ALL
from V$DATABASE;
```
To verify **supplemental logging** settings at table level:

```
select LOG_GROUP_NAME, TABLE_NAME, DECODE(ALWAYS, 'ALWAYS', 'Unconditional', NULL, 'Conditional') ALWAYS
from DBA_LOG_GROUPS;
```

### Creating non-privileged Oracle user for running LogMiner

Instructions below are for CDB, for non-CDB ([depreciated in 12c](Deprecation of Non-CDB Architecture), will be desupported in 20c) you can use role and user names without **c##** prefix.
Log in as sysdba and enter the following commands to create a user with the privileges required for running **oracdc** with LogMiner as CDC source. For CDB:

```
create user C##ORACDC identified by ORACDC
  default tablespace SYSAUX
  temporary tablespace TEMP
  quota unlimited on SYSAUX
CONTAINER=ALL;
alter user C##ORACDC SET CONTAINER_DATA=ALL CONTAINER=CURRENT;
grant
  CREATE SESSION,
  SET CONTAINER,
  SELECT ANY TRANSACTION,
  SELECT ANY DICTIONARY,
  EXECUTE_CATALOG_ROLE,
  LOGMINING
to C##ORACDC
CONTAINER=ALL;
```

For non-CDB:

```
create user ORACDC identified by ORACDC
  default tablespace SYSAUX
  temporary tablespace TEMP
  quota unlimited on SYSAUX;
grant
  CREATE SESSION,
  SELECT ANY TRANSACTION,
  SELECT ANY DICTIONARY,
  EXECUTE_CATALOG_ROLE,
  LOGMINING
to ORACDC CONTAINER=ALL;
```


### Additional configuration for physical standby database (V$DATABASE.OPEN_MODE = MOUNTED)

Connection to physical standby database when database is opened in **MOUNTED** mode is possible only for users for SYSDBA privilege. To check for correct user settings log in to SQL*Plus as SYSDBA and connect to physical standby database. To verify that you connected to physical standby database enter

```
select OPEN_MODE, DATABASE_ROLE, DB_UNIQUE_NAME from V$DATABASE;
```
it should return **MOUNTED** **PHYSICAL STANDBY** **<UNIQUE DATABASE NAME>**
Then enter:

```
select USERNAME from V$PWFILE_USERS where SYSDBA = 'TRUE';
```
For the user who will be used to connect to physical standby database create a Oracle Wallet. Please refer to section **Oracle Wallet** above.
To run **oracdc** in this mode parameter `a2.standby.activate` must set to `true`.

### Additional configuration for distributed database

1. Copy *oracdc-kafka-<VERSION>-standalone.jar* or *a2solutions-oracdc-kafka-<VERSION>.zip* to source and target (mining) database servers.
2. On source database server start _eu.solutions.a2.cdc.oracle.utils.file.SourceDatabaseShipmentAgent_ with _--bind-address_ (IP address or hostname to listen for incoming requests from mining database server agent, default **0.0.0.0**) and _--port_ (TCP port to listen for incoming requests from mining database server agent, default **21521**) parameters, for instance

```
java -cp oracdc-kafka-0.9.8-standalone.jar \
    eu.solutions.a2.cdc.oracle.utils.file.SourceDatabaseShipmentAgent \
        --port 21521 \
        --bind-address 192.168.7.101
```
3. On target (mining) database server start _eu.solutions.a2.cdc.oracle.utils.file.TargetDatabaseShipmentAgent_ with _--bind-address_ (IP address or hostname to listen for incoming requests from **oracdc** connector, default **0.0.0.0**), _--port_ (TCP port to listen for incoming requests from **oracdc** connector, default **21521**) parameters, _--source-host_ (IP address or hostname of _eu.solutions.a2.cdc.oracle.utils.file.SourceDatabaseShipmentAgent_), _--source-port_ (TCP port of _eu.solutions.a2.cdc.oracle.utils.file.SourceDatabaseShipmentAgent_), and _--file-destination_ (existing directory to store redo log files) for instance

```
java -cp oracdc-kafka-0.9.8-standalone.jar \
    eu.solutions.a2.cdc.oracle.utils.file.TargetDatabaseShipmentAgent
        --port 21521
        --bind-address 192.168.7.102
        --source-host 192.168.7.101
        --source-port 21521
        --file-destination /d00/oradata/archive
```
4. Configure **oracdc** connector with parameter `a2.distributed.activate` set to true.
Set `a2.jdbc.url`/`a2.jdbc.username`/`a2.jdbc.password` or `a2.wallet.location`/`a2.tns.admin`/`a2.tns.alias`/`a2.tns.alias` parameters to valid values for connecting to source database. Set `a2.distributed.wallet.location`/`a2.distributed.tns.admin`/`a2.distributed.tns.alias` to valid values for connecting to target (mining) database. Set `a2.distributed.target.host` and `a2.distributed.target.port` to IP address/hostname and port where _eu.solutions.a2.cdc.oracle.utils.file.TargetDatabaseShipmentAgent_ runs. Example parameter settings is in [logminer-source-distributed-db.properties](config/logminer-source-distributed-db.properties) file

## Materialized View logs as CDC source (eu.solutions.a2.cdc.oracle.OraCdcSourceConnector)

Create materialized view log's over replicated tables. These materialized view logs _must_ be created _with_ following options - **with primary key**, _and/or_ **with rowid**, **sequence**, **excluding new values** and _without_ **commit scn** option. You do not need to specify _column list_ while creating materialized view log for using with **oracdc**. **oracdc** reads from materialized view log only primary key value and/or rowid of row in master table.  Table below describes how **oracdc** operates depending on the materialized view log settings

|SNAPSHOT LOG WITH    |Master table access|Key for External system            |
|:--------------------|:------------------|:----------------------------------|
|Primary key          |Primary key        |Primary key                        |
|Primary key and ROWID|ROWID              |Primary key                        |
|ROWID                |ROWID              |ROWID (String, key name ORA_ROW_ID)|


```
connect scott/tiger
CREATE TABLE DEPT
       (DEPTNO NUMBER(2) CONSTRAINT PK_DEPT PRIMARY KEY,
        DNAME VARCHAR2(14) ,
        LOC VARCHAR2(13) );

CREATE TABLE EMP
       (EMPNO NUMBER(4) CONSTRAINT PK_EMP PRIMARY KEY,
        ENAME VARCHAR2(10),
        JOB VARCHAR2(9),
        MGR NUMBER(4),
        HIREDATE DATE,
        SAL NUMBER(7,2),
        COMM NUMBER(7,2),
        DEPTNO NUMBER(2) CONSTRAINT FK_DEPTNO REFERENCES DEPT);

create materialized view log on DEPT
  with primary key, sequence
  excluding new values;

create materialized view log on EMP
  with primary key, sequence
  excluding new values;

```

## oracdc Connector's parameters
For instructions **oracdc** Connector's parameters please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md).

## Message format

Every message is envelope has two parts: a *schema* and *payload*. The schema describes the structure of the payload, while the payload contains the actual data. Message format is similar to format used by [Debezium](https://debezium.io/) with two important differences:
1) *before* part of *schema* and *payload* used only for **DELETE** operation
2) for **DELETE** operation *schema* and *payload* contain only primary key definition and value due to limit of information from materialized view log
3) for **INSERT** and **UPDATE** only *after* part of payload contain values due to absense of row previous state in materialized view log's

### Message examples
See the [MESSAGE-SAMPLES-DBZM-STYLE.md](doc/MESSAGE-SAMPLES-DBZM-STYLE.md) for details

## Oracle RDBMS specific information
*source* structure of payload contains various information about Oracle RDBMS instance from V$INSTANCE and V$DATABASE views.
When using materialized view log as CDC source *scn* field for **INSERT** and **UPDATE** operations contains actual *ORA_ROWSCN* pseudocolumn value for given row in master table, for **DELETE** operation -  *ORA_ROWSCN* pseudocolumn value for given row in materialized view log.

### Oracle RDBMS Type mapping


By default (when `a2.oracdc.schemas` set to false) **oracdc** Source connector's is compatible with [Confluent JDBC Sink connector](https://docs.confluent.io/current/connect/kafka-connect-jdbc/sink-connector/index.html) and uses datatype mapping below
|Oracle RDBMS Type|JSON Type|Comment                                                       |
|:----------------|:--------|:-------------------------------------------------------------|
|DATE             |int32    |org.apache.kafka.connect.data.Date                            |
|TIMESTAMP%       |int64    |org.apache.kafka.connect.data.Timestamp                       |
|NUMBER           |int8     |NUMBER(1,0) & NUMBER(2,0)                                     |
|NUMBER           |int16    |NUMBER(3,0) & NUMBER(4,0)                                     |
|NUMBER           |int32    |NUMBER(5,0) & NUMBER(6,0) & NUMBER(7,0) & NUMBER(8,0)         |
|NUMBER           |int64    |Other Integers between billion and 1,000,000,000,000,000,000  |
|NUMBER           |float64  |Oracle NUMBER without specified SCALE and PRECISION           |
|NUMBER           |bytes    |org.apache.kafka.connect.data.Decimal - all other numerics    |
|FLOAT            |float64  |                                                              |
|BINARY_FLOAT     |float32  |                                                              |
|BINARY_DOUBLE    |float64  |                                                              |
|RAW              |bytes    |                                                              |
|BLOB             |bytes    |                                                              |
|CHAR             |string   |                                                              |
|NCHAR            |string   |                                                              |
|VARCHAR2         |string   |                                                              |
|NVARCHAR2        |string   |                                                              |

When `a2.oracdc.schemas` set to true **oracdc** uses its own extensions for Oracle **NUMBER** (**eu.solutions.a2.cdc.oracle.data.OraNumber**) and  **TIMESTAMP WITH [LOCAL] TIMEZONE** (**eu.solutions.a2.cdc.oracle.data.OraTimestamp**) datatypes.

When `a2.process.lobs` set to true **oracdc** uses its own extensions for Oracle **BLOB** (**eu.solutions.a2.cdc.oracle.data.OraBlob**), **CLOB** (**eu.solutions.a2.cdc.oracle.data.OraClob**),   **NCLOB** (**eu.solutions.a2.cdc.oracle.data.OraNClob**), and **SYS.XMLTYPE** (**eu.solutions.a2.cdc.oracle.data.OraXmlBinary**) datatypes.


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## TODO

* Support for _SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS/SUPPLEMENTAL LOG DATA (UNIQUE) COLUMNS_ to minimize supplemental logging overhead
* **oracdc** as audit information source
* Oracle LOB handler: convert Oracle BLOB/CLOB/BFILE to link on object file system and send ref to Kafka instead of LOB data

## Version history

####0.9.0 (OCT-2019)

Initial release

####0.9.1 (NOV-2019)

Oracle Wallet support for storing database credentials

####0.9.2 (DEC-2019)

"with ROWID" materialized view log support

####0.9.3 (FEB-2020)

[Oracle Log Miner](https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/oracle-logminer-utility.html) as CDC source
Removed AWS Kinesis support
New class hierarchy

#####0.9.3.1 (FEB-2020)

Removing dynamic invocation of Oracle JDBC. Ref.: [Oracle Database client libraries for Java now on Maven Central](https://blogs.oracle.com/developers/oracle-database-client-libraries-for-java-now-on-maven-central)


####0.9.4 (MAR-2020)

Ability to run [Oracle Log Miner](https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/oracle-logminer-utility.html) on the physical database when V$DATABASE.OPEN_MODE = MOUNTED to reduce TCO

#####0.9.4.1 (MAR-2020)

Persistence across restarts
CDB fixes/20c readiness


####0.9.5 (APR-2020)

Schema Editor GUI preview (java -cp <> eu.solutions.a2.cdc.oracle.schema.TableSchemaEditor). This GUI required for more precise mapping between Oracle and Kafka Connect datatypes. See also `a2.dictionary.file` parameter

####0.9.6 (MAY-2020)

Initial data load support. See also `a2.initial.load` parameter

#####0.9.6.1 (MAY-2020)

Partitioned tables support

#####0.9.6.2 (AUG-2020)

Oracle NUMBER datatype mapping fixes

#####0.9.6.3 (AUG-2020)

Kafka topic name configuration using `a2.topic.name.style` & `a2.topic.name.delimiter` parameters

#####0.9.6.4 (SEP-2020)

Dynamic list of tables to mine using `a2.table.list.style` parameter

####0.9.7 (OCT-2020)

LOB support. See also `a2.process.lobs` parameter

#####0.9.7.1 (OCT-2020)

Support for NCLOB

#####0.9.7.2 (NOV-2020)

VIP (Verified Integration Program) compliance

#####0.9.7.3 (NOV-2020)

fix CDB column type detection issue

#####0.9.7.4 (NOV-2020)

Important fixes for CDB

#####0.9.7.5 (JAN-2021)

fix Confluent Control Center 6.0+ sync issue with UCP PoolDataSource

#####0.9.7.6 (FEB-2021)

Protobuf Schema compatibility

#####0.9.7.7 (APR-2021)

Add more information about source record (XID, ROWID, and COMMIT_SCN)

#####0.9.7.8 (MAY-2021)

MAY-21 features/fixes (fix ORA-2396, add lag to JMX metrics, add feth size parameter)

####0.9.8 (JUL-2021)

Distributed database configuration

#####0.9.8.1 (AUG-2021)

RDBMS 21c compatibility

#####0.9.8.2 (OCT-2021)

SYS.XMLTYPE support and fixes for partitioned tables with BLOB/CLOB/NCLOB columns

#####0.9.8.3 (NOV-2021)

Large objects (BLOB/CLOB/NCLOB/XMLTYPE) transformation in source connector

## Authors

* **Aleksej Veremeev** - *Initial work* - [A2 Rešitve d.o.o.](http://a2-solutions.eu/)

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details

