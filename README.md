# oracdc

**oracdc** is a software package for near real-time data integration and replication in heterogeneous IT environments. **oracdc** consist of two  [Apache Kafka](http://kafka.apache.org/) Source Connector's and JDBC sink connector. **oracdc** provides data integration, [transactional change data capture](https://en.wikipedia.org/wiki/Change_data_capture), and data replication between operational and analytical IT systems. 
Starting from Oracle RDBMS 12c various Oracle tools for [CDC](https://en.wikipedia.org/wiki/Change_data_capture) and/or replication are [deprecated and desupported](https://docs.oracle.com/database/121/UPGRD/deprecated.htm) and replaced by [Oracle Golden Gate](https://www.oracle.com/middleware/technologies/goldengate.html). This project is not intended to be 100% replacement of [expensive](https://www.oracle.com/assets/technology-price-list-070617.pdf) Oracle Golden Gate licenses however may help in many practical cases. Project was tested using [Oracle E-Business Suite](https://www.oracle.com/applications/ebusiness/) customer instance for transferring various information (mostly INV, ONT, WSH, GL & XLA tables) to further reporting and analytics in [PostgreSQL](https://www.postgresql.org/) database. We tested both short transactions (human data entry) and long transactions (various accounting programs) changing millions of rows in dozens of tables (description of some tables used: [WSH_NEW_DELIVERIES](https://docs.oracle.com/cd/E51367_01/scmop_gs/OEDSC/WSH_NEW_DELIVERIES_tbl.htm), [WSH_DELIVERY_ASSIGNMENTS](https://docs.oracle.com/cd/E51367_01/scmop_gs/OEDSC/WSH_DELIVERY_ASSIGNMENTS_tbl.htm), [WSH_DELIVERY_DETAILS](https://docs.oracle.com/cd/E51367_01/scmop_gs/OEDSC/WSH_DELIVERY_DETAILS_tbl.htm)).
**oracdc** Source Connector's compatible with [Oracle RDBMS](https://www.oracle.com/database/index.html) versions 10g, 11g, 12c, 18c, 19c, 21c, 23c, and 23ai. If you need support for Oracle Database 9i and please send us an email at [oracle@a2-solutions.eu](mailto:oracle@a2-solutions.eu).

## solutions.a2.cdc.oracle.OraCdcLogMinerConnector
This Source Connector uses [Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html) as source for data changes. Connector is designed to minimize the side effects of using Oracle LogMiner, even for Oracle RDBMS versions with **DBMS_LOGMNR.CONTINUOUS_MINE** feature support **oracdc** does not use it. Instead, **oracdc** reads **V$LOGMNR_CONTENTS** and saves information with **V$LOGMNR_CONTENTS.OPERATION in ('INSERT', 'DELETE', 'UPDATE')** in Java off-heap memory structures provided by [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue). This approach minimizes the load on the Oracle database server, but requires additional disk space on the server with **oracdc** installed. Since version **2.0**, the connector supports online redo log processing when `a2.process.online.redo.logs` is set to **true**. Starting from version **1.5** in addition to [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue), the use of Java Heap Structures is also supported. Large object operations are not supported in this mode, but you do not need any disk space to store the [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) in memory-mapped files. To enable this mode you need to set `a2.transaction.implementation=ArrayList` and also you need to set Java heap size to the appropriate value using JVM [-Xmx](https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html) option.

**oracdc**'s _solutions.a2.cdc.oracle.OraCdcLogMinerConnector_ connects to the following configurations of Oracle RDBMS:
1. Standalone instance, or Primary Database of Oracle DataGuard Cluster/Oracle Active DataGuard Cluster, i.e. **V$DATABASE.OPEN_MODE = READ WRITE** 
2. Physical Standby Database of Oracle **Active DataGuard** cluster, i.e. **V$DATABASE.OPEN_MODE = READ ONLY**
3. Physical Standby Database of Oracle **DataGuard** cluster, i.e. **V$DATABASE.OPEN_MODE = MOUNTED**. In this mode, a physical standby database is used to retrieve data using LogMiner and connection to primary database is used to perform strictly limited number of queries to data dictionary (ALL|CDB_OBJECTS, ALL|CDB_TABLES, and ALL|CDB_TAB_COLUMNS). When running against single instance physical standby for Oracle RAC connector automatically detects opened redo threads and starts required number of connector tasks (max.tasks parameter must be equal to or greater than the number of redo threads). This option allows you to promote a physical standby database to source of replication, eliminates LogMiner overhead from primary database, and  decreases TCO of Oracle Database.
4. Running in distributed configuration when the source database generates redo log files and also contains a dictionary and target database is a compatible mining database (see Figure 25-1 in [Using LogMiner to Analyze Redo Log Files](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html)). **N.B.** Currently only non-CDB distributed database configuration has been tested, tests for CDB distributed database configuration are in progress now.
5. [Oracle RAC](https://www.oracle.com/database/real-application-clusters/) Database. For a detailed description of how to configure **oracdc** to work with [Oracle RAC](https://www.oracle.com/database/real-application-clusters/) please see [What about Oracle RAC?](https://github.com/averemee-si/oracdc/wiki/What-about-Oracle-RAC%3F).

### Monitoring
_solutions.a2.cdc.oracle.OraCdcLogMinerConnector_ publishes a number of metrics about the connector’s activities that can be monitored through JMX. For complete list of metrics please refer to [LOGMINER-METRICS.md](doc/LOGMINER-METRICS.md)

### Oracle Database SecureFiles and Large Objects including SYS.XMLTYPE
Oracle Database SecureFiles and Large Objects i.e. LOB's are supported from v0.9.7 when parameter `a2.process.lobs` set to true. CLOB type supported only for columns with **DBA_LOBS.FORMAT='ENDIAN NEUTRAL'**. If you need support for CLOB columns with **DBA_LOBS.FORMAT='ENDIAN SPECIFIC'** or **XMLTYPE** please send us an email at [oracle@a2-solutions.eu](mailto:oracle@a2-solutions.eu).
SYS.XMLTYPE data are supported from v0.9.8.2 when parameter `a2.process.lobs` set to true.
For processing LOB's and SYS.XMLTYPE please do not forget to set Apache Kafka parameters according to size of LOB's:
1. For Source connector: _producer.max.request.size_
2. For broker: _replica.fetch.max.bytes_ and _message.max.bytes_
By default CLOB, NCLOB, and SYS.XMLTYPE data are compressed using [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression algorithm.

#### Large objects (BLOB/CLOB/NCLOB/XMLTYPE) operations LOB_TRIM and LOB_ERASE
[Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html) generate unparseable (at moment) output when operates without connection to dictionary. Currently **oracdc** ignores these operations. Starting from **oracdc** v1.2.2 additional debug information about these operations is printed to log. If you need support for these operations please send us an email at [oracle@a2-solutions.eu](mailto:oracle@a2-solutions.eu)

#### Large objects (BLOB/CLOB/NCLOB/XMLTYPE) transformation feature (oracdc 0.9.8.3+)
Apache Kafka is not meant to handle large messages with size over 1MB. But Oracle RDBMS often is used as storage for unstructured information too. For breaking this barrier we designed LOB transformation features.
Imagine that you have a table with following structure

```
describe APPLSYS.FND_LOBS
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

import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
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
										.bucket(<VALID_S3_BUCKET>)
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
shows correct values in redo block. This checked using all available Oracle RDBMS 21c patchsets and Oracle RDBMS 23c (21.3, 21.4, 21.5, 21.6, 23.2, and 23.3).
Unfortunately, JSON data type is not contained in LogMiner's [Supported Data Types and Table Storage Attributes](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html#GUID-D11CC6EF-D94C-426F-B244-96CE2403924A), nor in the [Unsupported Data Types and Table Storage Attributes](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html#GUID-8A4F98EC-C233-4471-BFF9-9FB35EF5AD0D).
We are watching the status of this issue.


#### BOOLEAN Datatype support for Oracle RDBMS 23c +
Unfortunately, for operations on the BOOLEAN data type, the result is returned as (below is results for transaction which includes BOOLEAN datatype)

```
select SQL_REDO from V$LOGMNR_CONTENTS where XID='0600200038020000';

SQL_REDO
--------------------------------------------------------------------------------
set transaction read write
Unsupported
Unsupported
commit

6 rows selected.

```
Wherein

```
alter system dump logfile '/path-to-archived-log-file' scn min XXXXXXXXX scn max YYYYYYYYY;
```
shows correct values in redo block. This checked using Oracle RDBMS 23c (23.2,  23.3, & 23.4/23AI).
Unfortunately, BOOLEAN data type is not contained in LogMiner's [Supported Data Types and Table Storage Attributes](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html#GUID-D11CC6EF-D94C-426F-B244-96CE2403924A), nor in the [Unsupported Data Types and Table Storage Attributes](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html#GUID-8A4F98EC-C233-4471-BFF9-9FB35EF5AD0D).
We are watching the status of this issue.

### DDL Support and schema evolution
The following [Data Definition Language (DDL)](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Types-of-SQL-Statements.html#GUID-FD9A8CB4-6B9A-44E5-B114-EFB8DA76FC88) clauses of [ALTER TABLE](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/ALTER-TABLE.html#GUID-552E7373-BF93-477D-9DA3-B2C9386F2877) command are currently supported:

```
1. add column(s)
2. modify column(s)
3. drop column(s)
4. rename column
5. set unused column(s)
```
To ensure compatibility with [Schema Evolution](https://docs.confluent.io/current/schema-registry/avro.html#schema-evolution) the following algorithm is used:
1. When **oracdc** first encounters an operation on a table in the redo logs, information about the table is read from the [data dictionary](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/data-dictionary-and-dynamic-performance-views.html#GUID-9B9ABE1C-A1E3-464F-8936-978250DC3E1F) or from the JSON file specified by the  `a2.dictionary.file` parameter. Two schemas are created: immutable key schema with unique identifier **[PDB_NAME:]OWNER.TABLE_NAME.Key**, **version=1** and mutable value schema with unique identifier **[PDB_NAME:]OWNER.TABLE_NAME.Value**, **version=1**. 
2. After successful parsing of [DDL](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Types-of-SQL-Statements.html#GUID-FD9A8CB4-6B9A-44E5-B114-EFB8DA76FC88) and comparison of columns definition before and after, value schema gets an incremented version number.

### RDBMS errors resiliency and connection retry back-off
**oracdc** is resilient to Oracle database shutdown and/or restart while performing [DBMS_LOGMNR.ADD_LOGFILE](https://docs.oracle.com/en/database/oracle/oracle-database/23/arpls/DBMS_LOGMNR.html#GUID-30C5D959-C0A0-4591-9FBA-F57BC72BBE2F) call or waiting for a new archived redo log. ORA-17410 ("No more data read from socket") is intercepted and an attempt to reconnect is made after fixed backoff time specified by parameter `a2.connection.backoff`

### Kafka Connect distributed mode
Fully compatible from 0.9.9.2+, when `a2.resiliency.type` set to ``fault-tolerant``. `In this case all offset (SCN, RBA, COMMIT_SCN for rows and transactions, versions for table definitions) information is stored only on standard Kafka Connect offsets

### Known issues
1. Zillions of messages (for every Oracle database transaction)

```
[2023-12-15 12:00:41,214] INFO [oracdc-test|task-0] Took 0.371 ms to pollDiskSpace for /tmp/7F00180051B30100.9949840719368541210 (net.openhft.chronicle.threads.DiskSpaceMonitor:56)

```

in connect.log.
These messages are generated by [Chronicle Queue's](https://github.com/OpenHFT/Chronicle-Queue) [DiskSpaceMonitor](https://github.com/OpenHFT/Chronicle-Threads/blob/develop/src/main/java/net/openhft/chronicle/threads/DiskSpaceMonitor.java).
To completely disable this diagnostic output, set
 
```
export KAFKA_OPTS="-Dchronicle.disk.monitor.disable=true ${KAFKA_OPTS}"
```
 
 for the JVM running Kafka Connect.
 If you are interested in the statistics of waits related to memory mapped files but want to set a different threshold (default is 250 milliseconds), then set (example below is for threshold of 500 milliseconds)
 
```
export KAFKA_OPTS="-Dchronicle.disk.monitor.warn.threshold.us=500 ${KAFKA_OPTS}"
```

2. The default **oracdc** option [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) for processing Oracle transaction information [collects statistics](https://github.com/OpenHFT/Chronicle-Queue/blob/ea/DISCLAIMER.adoc). If you do not consent you can do either of the following:

A) Switch from using [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) to use Java Heap Structures by setting `a2.transaction.implementation=ArrayList` together with proper sizing of Java Heap Size using JVM [-Xmx](https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html) option.

**OR**

B) Follow [instructions from Chronicle](https://github.com/OpenHFT/Chronicle-Queue/blob/ea/DISCLAIMER.adoc)


3. Excessive LogMiner output in alert.log - please see Oracle Support Services Notes [LOGMINER: Summary For Session# = nnn in 11g (Doc ID 1632209.1)](https://support.oracle.com/rs?type=doc&id=1632209.1) and [Alert.log Messages 'LOGMINER: krvxpsr summary' (Doc ID 1485217.1)](https://support.oracle.com/rs?type=doc&id=1485217.1)


### Performance tips
1. If you do not use archivelogs as a source of database user activity audit information, consider setting Oracle RDBMS hidden parameter **_transaction_auditing** to **false** after consulting a [Oracle Support Services](https://www.oracle.com/support/)
2. Always try to set up **supplemental logging** at the table level, and not for all database objects
3. Proper file system parameters and sizing for path where [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) objects reside.
4. Proper open files hard and soft limits for OS user running **oracdc**
5. To determine source of bottleneck set parameter `a2.logminer.trace` to true and analyze waits at Oracle RDBMS side using data from trace file (**tracefile_identifier='oracdc'**)
6. For optimizing network transfer consider increasing SDU (Ref.: [Database Net Services Administrator's Guide, Chapter 14 "Optimizing Performance"](https://docs.oracle.com/en/database/oracle/oracle-database/23/netag/optimizing-performance.html)). Also review Oracle Support Services Note 2652240.1[SDU Ignored By The JDBC Thin Client Connection](https://support.oracle.com/epmos/faces/DocumentDisplay?id=2652240.1).
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

Example sqlnet.ora with SDU set:

```
DEFAULT_SDU_SIZE = 2097152
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
9. Although some documents recommend changing the setting of _log_read_buffers & _log_read_buffer_size hidden parameters we didn't see serious improvement or degradation using different combinations of these parameters. For more information please read [LogMiner Tuning: _log_read_buffers & _log_read_buffer_size](https://github.com/averemee-si/oracdc/wiki/LogMiner-Tuning:-_log_read_buffers-&-_log_read_buffer_size)
10. **oracdc** uses off-heap storage [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) developed by the [Chronicle Software](https://chronicle.software/). To determine required disk space, or size of [Docker's](https://www.docker.com/) [tmpfs mounts](https://docs.docker.com/storage/tmpfs/), or size of k8s's [emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/) needed to store memory allocated files use values of following [JMX metrics]((doc/LOGMINER-METRICS.md))

```
MaxTransactionSizeMiB
MaxNumberOfTransInProcessingQueue
GiBWrittenUsingChronicleQueue
```
When setting the JVM parameters , pay attention to the Linux kernel parameter `vm.max_map_count` and to JVM parameter `-XX:MaxDirectMemorySize`. (Ref.: [I have issue with memory](https://github.com/averemee-si/oracdc/issues/8#issuecomment-725227858))
11. Run oracdc using [Java 17 LTS](https://www.oracle.com/java/technologies/java-se-support-roadmap.html), which provides numerous improvements over older versions of Java, such as improved [performance](https://chronicle.software/which-is-the-fastest-jvm/), stability, and security. For this you need to pass additional command line arguments to java command using (Ref.: [How to run Chronicle Libraries Under Java 17](https://chronicle.software/chronicle-support-java-17/))

```
export KAFKA_OPTS="\
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
--add-exports java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports jdk.unsupported/sun.misc=ALL-UNNAMED \
--add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
--add-opens jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
--add-opens java.base/java.lang=ALL-UNNAMED \
--add-opens java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens java.base/java.io=ALL-UNNAMED \
--add-opens java.base/java.util=ALL-UNNAMED"
```

12. Please review [Latest GoldenGate/Database (OGG/RDBMS) Patch recommendations (Doc ID 2193391.1)](https://support.oracle.com/rs?type=doc&id=2193391.1) (for 11.2.0.3/11/2/0.4 - [Oracle GoldenGate -- Oracle RDBMS Server Recommended Patches](https://support.oracle.com/rs?type=doc&id=1557031.1)) and, if necessary, install the fixes listed in the **Oracle RDBMS software** section. If you are using Oracle Database 19c, be sure to install the patch [Patch 35034699: RTI 25675772 (TKLSDDN01V.DIFF) DIVERGENCE MAY OCCUR IN DISTRIBUTED TXN WITH PARTIAL ROLLBACK](https://support.oracle.com/epmos/faces/PatchDetail?patchId=35034699)

### Distribution
1. [GitHub](https://github.com/averemee-si/oracdc/)
2. [Confluent Hub](https://www.confluent.io/hub/a2solutions/oracdc-kafka)
3. [DockerHub](https://hub.docker.com/r/averemee/oracdc)
4. [AWS Marketplace](https://aws.amazon.com/marketplace/seller-profile?id=07173d1d-0e5c-4e97-8db7-5c701176865c) - optimized for [Amazon MSK](https://aws.amazon.com/msk/) and [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
4.1. [x86_64 CloudFormation Stack](https://aws.amazon.com/marketplace/pp/prodview-cqki33xso6s6a)
4.2. [AWS Graviton CloudFormation Stack](https://aws.amazon.com/marketplace/pp/prodview-fn6bpplwp4m6y)
4.3. [amd64 Container](https://aws.amazon.com/marketplace/pp/prodview-me6ugntrriqeg)
5. [Maven Central](https://mvnrepository.com/artifact/solutions.a2.oracle/oracdc-kafka/)

## solutions.a2.cdc.oracle.OraCdcSourceConnector
This Source Connector uses Oracle RDBMS [materialized view log's](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/CREATE-MATERIALIZED-VIEW-LOG.html) as source for data changes and materializes Oracle RDBMS materialized view log at heterogeneous database system. No materialized view should consume information from materialized view log's which are used by **oracdc**. Unlike _solutions.a2.cdc.oracle.OraCdcLogMinerConnector_ this SourceConnector works with BLOB, and CLOB data types. If you need support for Oracle Database _LONG_, and/or _LONG RAW_ data types please send us an email at [oracle@a2-solutions.eu](mailto:oracle@a2-solutions.eu).
In addition to read privileges on the underlying base tables and materialized view logs, the user running connector must have access to

```
grant select on V_$INSTANCE to <CONNECTOR-USER>;
grant select on V_$LICENSE to <CONNECTOR-USER>;
grant select on V_$DATABASE to <CONNECTOR-USER>;
```


# Docker Container
For a quick start with the Docker container, please read [How to Set Up Oracle Database Tables Replication in Minutes](https://averemee.substack.com/p/how-to-set-up-oracle-database-tables)


# Getting Started

These instructions will get you a copy of the project up and running on any platform with JDK11+ support.

## Prerequisites

Before using **oracdc** please check that required Java11+ is installed with

```
echo "Checking Java version"
java -version
```

## Installing

Build with

```
mvn clean install -Dgpg.skip
```

### Oracle JDBC drivers

**oracdc** is shipped with Oracle JDBC 21.9.0.0, or you can copy drivers from Oracle RDBMS server

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

## Oracle LogMiner as CDC source (solutions.a2.cdc.oracle.OraCdcLogMinerConnector)
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

Instructions below are for CDB, for non-CDB ([deprecated in 12c](https://support.oracle.com/epmos/faces/DocumentDisplay?id=2808317.1), desupported in 21c) you can use role and user names without **c##** prefix.
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

For non-CDB or for connection to PDB in RDBMS 19.10+:

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
to ORACDC;
```

### Oracle Database settings check utility
To check the required Oracle Database settings, you can use the setup check utility (v2.2.+), which will check all the settings and, if there are problems, advise on how to fix them

```
java -cp oracdc-kafka-2.2.0-standalone.jar solutions.a2.cdc.oracle.utils.OracleSetupCheck \
     --jdbc-url <ORA-JDBC-URL> --user <ACCOUNT-TO-RUN-CONNECTOR> --password <ACCOUNT-PASSWORD>
```

To run in container environment

```
docker run --rm -it averemee/oracdc oraCheck.sh \
     --jdbc-url <ORA-JDBC-URL> --user <ACCOUNT-TO-RUN-CONNECTOR> --password <ACCOUNT-PASSWORD>
```
If there are no problems with the settings, the utility prints the following output

```
=====================

The oracdc's setup check was completed successfully, everything is ready to start oracdc connector

=====================
```


### Options for connecting to Oracle Database
In CDB Architecture **oracdc** must connected to [CDB$ROOT](https://docs.oracle.com/en/database/oracle/oracle-database/23/multi/introduction-to-the-multitenant-architecture.html), but starting from Oracle Database [19c RU 10](https://updates.oracle.com/download/32218454.html) and Oracle Database 21c you can chose to connect either to the [CDB$ROOT](https://docs.oracle.com/en/database/oracle/oracle-database/23/multi/introduction-to-the-multitenant-architecture.html), or to an individual PDB.

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
2. On source database server start _solutions.a2.cdc.oracle.utils.file.SourceDatabaseShipmentAgent_ with _--bind-address_ (IP address or hostname to listen for incoming requests from mining database server agent, default **0.0.0.0**) and _--port_ (TCP port to listen for incoming requests from mining database server agent, default **21521**) parameters, for instance

```
java -cp oracdc-kafka-0.9.8-standalone.jar \
    solutions.a2.cdc.oracle.utils.file.SourceDatabaseShipmentAgent \
        --port 21521 \
        --bind-address 192.168.7.101
```
3. On target (mining) database server start _solutions.a2.cdc.oracle.utils.file.TargetDatabaseShipmentAgent_ with _--bind-address_ (IP address or hostname to listen for incoming requests from **oracdc** connector, default **0.0.0.0**), _--port_ (TCP port to listen for incoming requests from **oracdc** connector, default **21521**) parameters, _--source-host_ (IP address or hostname of _solutions.a2.cdc.oracle.utils.file.SourceDatabaseShipmentAgent_), _--source-port_ (TCP port of _solutions.a2.cdc.oracle.utils.file.SourceDatabaseShipmentAgent_), and _--file-destination_ (existing directory to store redo log files) for instance

```
java -cp oracdc-kafka-0.9.8-standalone.jar \
    solutions.a2.cdc.oracle.utils.file.TargetDatabaseShipmentAgent
        --port 21521
        --bind-address 192.168.7.102
        --source-host 192.168.7.101
        --source-port 21521
        --file-destination /d00/oradata/archive
```
4. Configure **oracdc** connector with parameter `a2.distributed.activate` set to true.
Set `a2.jdbc.url`/`a2.wallet.location` or `a2.jdbc.url`/`a2.jdbc.username`/`a2.jdbc.password` parameters to valid values for connecting to source database. Set `a2.distributed.jdbc.url`/`a2.distributed.wallet.location` to valid values for connecting to target (mining) database. Set `a2.distributed.target.host` and `a2.distributed.target.port` to IP address/hostname and port where _solutions.a2.cdc.oracle.utils.file.TargetDatabaseShipmentAgent_ runs. Example parameter settings is in [logminer-source-distributed-db.properties](config/logminer-source-distributed-db.properties) file

## Materialized View logs as CDC source (solutions.a2.cdc.oracle.OraCdcSourceConnector)

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
|INTERVALYM       |string   |solutions.a2.cdc.oracle.data.OraInterval                      |
|INTERVALDS       |string   |solutions.a2.cdc.oracle.data.OraInterval                      |
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

When `a2.oracdc.schemas` set to true **oracdc** uses its own extensions for Oracle **NUMBER** (**solutions.a2.cdc.oracle.data.OraNumber**), **TIMESTAMP WITH [LOCAL] TIMEZONE** (**solutions.a2.cdc.oracle.data.OraTimestamp**), **INTERVALYM(INTERVAL YEAR TO MONTH)** (**solutions.a2.cdc.oracle.data.OraIntervalYM**), and **INTERVALDS(INTERVAL DAY TO SECOND)** (**solutions.a2.cdc.oracle.data.OraIntervalDS**) datatypes.

When `a2.process.lobs` set to true **oracdc** uses its own extensions for Oracle **BLOB** (**solutions.a2.cdc.oracle.data.OraBlob**), **CLOB** (**solutions.a2.cdc.oracle.data.OraClob**),   **NCLOB** (**solutions.a2.cdc.oracle.data.OraNClob**), and **SYS.XMLTYPE** (**solutions.a2.cdc.oracle.data.OraXmlBinary**) datatypes.


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## TODO

* better resilience to RDBMS errors
* **oracdc** as audit information source
* better schema management including ideas from [timestamp of creation of a schema version](https://github.com/confluentinc/schema-registry/issues/1899)


## Version history

####0.9.0 (OCT-2019)

Initial release

####0.9.1 (NOV-2019)

Oracle Wallet support for storing database credentials

####0.9.2 (DEC-2019)

"with ROWID" materialized view log support

####0.9.3 (FEB-2020)

[Oracle Log Miner](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html) as CDC source
Removed AWS Kinesis support
New class hierarchy

#####0.9.3.1 (FEB-2020)

Removing dynamic invocation of Oracle JDBC. Ref.: [Oracle Database client libraries for Java now on Maven Central](https://blogs.oracle.com/developers/oracle-database-client-libraries-for-java-now-on-maven-central)


####0.9.4 (MAR-2020)

Ability to run [Oracle Log Miner](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html) on the physical database when V$DATABASE.OPEN_MODE = MOUNTED to reduce TCO

#####0.9.4.1 (MAR-2020)

Persistence across restarts
CDB fixes/20c readiness


####0.9.5 (APR-2020)

Schema Editor GUI preview (java -cp <> solutions.a2.cdc.oracle.schema.TableSchemaEditor). This GUI required for more precise mapping between Oracle and Kafka Connect datatypes. See also `a2.dictionary.file` parameter

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

#####0.9.8.4 (NOV-2021)

CDB: support connection to CDB$ROOT or to an individual PDB

####0.9.9 (JAN-2022)

DDL operations support for LogMiner source

#####0.9.9.1 (JAN-2022)

ORA-1291 fixes and new non-static connection pool for LogMiner connector

#####0.9.9.2 (FEB-2022)

`a2.resiliency.type` = ``fault-tolerant`` to ensure 100% compatibility with Kafka Connect distributed mode

####1.0.0 (JUN-2022)
Min Java version -> Java11, Java 17 LTS - recommended
Package name change: eu.solutions.a2 -> solutions.a2

####1.1.0 (AUG-2022)
Deprecation of parameters `a2.tns.admin`, `a2.tns.alias`, `a2.standby.tns.admin`, `a2.standby.tns.alias`, `a2.distributed.tns.admin`, and `a2.distributed.tns.alias`. Please use `a2.jdbc.url`, `a2.standby.jdbc.url`, and `a2.distributed.jdbc.url` respectively. Please see [KAFKA-CONNECT.md](https://github.com/averemee-si/oracdc/blob/master/doc/KAFKA-CONNECT.md) for parameter description and [Oracle® Database JDBC Java API Reference, Release 23c](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/) for more information about JDBC URL format.

####1.2.0 (SEP-2022)
Oracle RAC support, for more information please see [What about Oracle RAC?](https://github.com/averemee-si/oracdc/wiki/What-about-Oracle-RAC%3F)

#####1.2.1 (SEP-2022)
replace log4j with reload4j (CVE-2022-23305, CVE-2019-17571, CVE-2022-23302, CVE-2022-23307, CVE-2020-9488)

#####1.2.2 (OCT-2022)
LOB_TRIM/LOB_ERASE output to log & Jackson Databind version change (fix for [CVE-2022-42004](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-42004))

####1.3.0 (DEC-2022)
Single instance physical standby for Oracle RAC support

#####1.3.1 (JAN-2023)
fix for OCI DBCS product name like "Oracle Database 19c EE Extreme Perf"

#####1.3.2 (FEB-2023)
fix for https://github.com/averemee-si/oracdc/issues/40 & jackson library update

#####1.3.3 (MAR-2023)
techstack/dependent libraries (JUnit/commons-cli/OJDBC/SL4J) version updates

######1.3.3.1 (MAR-2023)
fix temporary dir check error when `a2.tmpdir` is not specified

######1.3.3.2 (MAR-2023)
parameter `a2.protobuf.schema.naming` for fixing issue with prtobuf identifiers

#####1.4.0 (MAY-2023)
Oracle 23c readiness, supplemental logging checks, fixes for Oracle RDBMS on Microsoft Windows

######1.4.1 (MAY-2023)
New `a2.schema.type=single` - schema type to store all columns from database row in one message with just value schema

######1.4.2 (JUN-2023)
fix unhandled ORA-17410 running 12c on Windows and more strict checks for supplemental logging settings

#####1.5.0 (AUG-2023)
New `a2.pk.string.length` parameter for Sink Connector and other Sink Connector enhancement
New `a2.transaction.implementation` parameter for LogMiner Source Connector: when set to `ChronicleQueue` (default) **oracdc** uses [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) to store information about SQL statements in Oracle transaction and uses off-heap memory and needs disk space to store memory mapped files; when set to `ArrayList` **oracdc** uses ArrayList to store information about SQL statements in Oracle transaction and uses JVM heap (no disk space needed).
Fix for ORA-17002 while querying data dictionary
Better handling for SQLRecoverableException while querying data dictionary

#####1.6.0 (OCT-2023)
Support for INTERVALYM/INTERVALDS
TIMESTAMP enhancements
SDU hint in log

####2.0.0 (DEC-2023)
######Online redo logs processing
Online redo logs are processed when parameter `a2.process.online.redo.logs` is set to **true** (Default - **false**). To control the lag between data processing in Oracle, the parameter `a2.scn.query.interval.ms` is used, which sets the lag in milliseconds for processing data in online logs.
This expands the range of connector tasks and makes its use possible where minimal and managed latency is required.

######default values:
Column default values are now part of table schema

######19c enhancements:
1) HEX('59') (and some other **single** byte values) for DATE/TIMESTAMP/TIMESTAMPTZ are treated as NULL
2) HEX('787b0b06113b0d')/HEX('787b0b0612013a')/HEX('787b0b0612090c')/etc (2.109041558E-115,2.1090416E-115,2.109041608E-115) for NUMBER(N)/NUMBER(P,S) are treated as NULL
information about such values is not printed by default in the log; to print messages you need to set the parameter `a2.print.invalid.hex.value.warning` value to **true**

######solution for incomplete redo information:
Solution for problem described in [LogMiner REDO_SQL missing WHERE clause](https://asktom.oracle.com/pls/apex/f?p=100:11:::::P11_QUESTION_ID:9544753100346374249) and [LogMiner Redo SQL w/o WHERE-clause](https://groups.google.com/g/debezium/c/0cTC-dQrxW8)

#####2.1.0 (FEB-2024)

ServiceLoader manifest files, for more information please read [KIP-898: Modernize Connect plugin discovery](https://cwiki.apache.org/confluence/display/KAFKA/KIP-898%3A+Modernize+Connect+plugin+discovery)

######LogMiner Connector
1) Now **oracdc** now also checks for first available SCN in V$LOG
2) Reducing the output about scale differences between redo and dictionary
3) Separate first available SCN detection for primary and standby

######New parameters
`a2.incomplete.redo.tolerance` - to manage connector behavior when processing an incomplete redo record. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)
`a2.print.all.online.scn.ranges` - to control output when processing online redo logs. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)
`a2.log.miner.reconnect.ms` - to manage reconnect interval for LogMiner for Unix/Linux. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)
`a2.pk.type` - to manage behavior when choosing key fields in schema for table.  For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)
`a2.use.rowid.as.key` - to manage behavior when the table does not have appropriate PK/unique columns for key fields.  For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)
`a2.use.all.columns.on.delete` - to manage behavior when reading and processing a redo record for DELETE. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)

######Sink Connector
1) fix for SQL statements creation when first statement in topic is "incomplete" (delete operation for instance)
2) add exponential back-off for sink getConnection()
3) support for key-less schemas
4) PostgreSQL: support for implicitly defined primary keys

#####2.2.0 (MAR-2024)

######LogMiner Connector
1) Enhanced handling of partial rollback redo records (ROLLBACK=1). For additional information about these redo records please read [ROLLBACK INTERNALS](https://blog.ora-600.pl/2017/09/20/rollback-internals/) starting with the sentence _"The interesting thing is with partial rollback."_
2) New parameter `a2.topic.mapper` to manage the name of the Kafka topic to which data will be sent. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)
3) Oracle Database settings check utility

######Sink Connector
1) Connector classes are re-factored and the Sink Connector itself renamed from **solutions.a2.cdc.oracle.OraCdcJdbcSinkConnector** to **solutions.a2.kafka.sink.JdbcSinkConnector**
2) New parameter - `a2.table.mapper` to manage the table in which to sink the data.

#####2.3.0 (APR-2024)

######LogMiner Connector

1) New parameter - `a2.stop.on.ora.1284` to manage the connector behavior on ORA-1284. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)
2) Checking the number of non-zero columns returned from a redo record for greater reliability.
3) Handling of partial rollback records in RDBMS 19.13 i.e. when redo record with ROLLBACK=1 is before redo record with ROLLBACK=0
4) Processing of DELETE operation for tables ROWID pseudo key
5) New parameter - `a2.print.unable.to.delete.warning` to manage the connector output in log for DELETE operations over table's without PK. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)
6) New parameter - `a2.schema.name.mapper` to manage schema names generation. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)

######Docker image
Rehost Confluent schema registry clients (Avro/Protobuf/JSON Schema) and bump version to 7.5.3

######2.3.1 (APR-2024)

Simplification of configuration for [Oracle Active DataGuard](https://www.oracle.com/database/data-guard/) - now the same configuration is used for [Oracle Active DataGuard](https://www.oracle.com/database/data-guard/) as for a primary database

#####2.4.0 (MAY-2024)

######LogMiner Connector

1) [Oracle Active DataGuard](https://www.oracle.com/database/data-guard/) support for Oracle Database settings check utility
2) Fix for [Oracle DataGuard](https://www.oracle.com/database/data-guard/) when V$STANDBY_LOG does not contain rows
3) Fix ORA-310/ORA-334 under heavy RDBMS load
4) New parameters to support pseudo columns - `a2.pseudocolumn.ora_rowscn`, `a2.pseudocolumn.ora_commitscn`, `a2.pseudocolumn.ora_rowts`, & `a2.pseudocolumn.ora_operation`. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)
5) New parameters to support audit pseudo columns: `a2.pseudocolumn.ora_username`, `a2.pseudocolumn.ora_osusername`, `a2.pseudocolumn.ora_hostname`, `a2.pseudocolumn.ora_audit_session_id`, `a2.pseudocolumn.ora_session_info`, & `a2.pseudocolumn.ora_client_id`. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md)


######Sink Connector

New parameters: `a2.table.mapper`, `a2.table.name.prefix`, and `a2.table.name.suffix`

#####2.5.0 (AUG-2024)

######LogMiner Connector

1) Improved processing of transactions containing [partial rollback](https://blog.ora-600.pl/2017/09/20/rollback-internals/) (with ROLLBACK=1) statements
2) JMX: LastProcessedSequence metric. For more information please read [LOGMINER-METRICS.md](doc/LOGMINER-METRICS.md)
3) Obsoleted and removed parameters: `a2.resiliency.type`, `a2.persistent.state.file`, `a2.redo.count`, `a2.redo.size`
4) New parameter to control the selection of database table columns to create key fields of a Kafka Connect record `a2.key.override`. For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md).
5) New parameter to add notifications about last processed redo sequence `a2.last.sequence.notifier`.  For more information please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md).

######Sink Connector

New parameter to set a SQL statement(s) that will be executed for all new connections when they are created - `a2.connection.init.sql`

######2.5.1 (AUG-2024)

#######LogMiner Connector

1) Handling/binding suspicious transactions (XID always ends with FFFFFFFF, i.e. wrong transaction ID sequence number) and the transaction always starts with a partial rollback operation
2) New parameter and additional pseudocolumn `a2.pseudocolumn.ora_xid`

######2.5.2 (SEP-2024)

1) SMT converters for
    solutions.a2.cdc.oracle.data.OraNumber/solutions.a2.cdc.oracle.data.OraIntervalYM/solutions.a2.cdc.oracle.data.OraIntervalDS (oracle.sql.NUMBER/oracle.sql.INTERVALYM/oracle.sql.INTERVALDS)
2) Dockerfile enhancements (Schema registry client updated to Confluent 7.7.1), Dockerfile.snowflake to quickly create a data delivery pipeline between transactional Oracle and analytical Snowflake


## Authors

* **Aleksej Veremeev** - *Initial work* - [A2 Rešitve d.o.o.](http://a2-solutions.eu/)

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details

