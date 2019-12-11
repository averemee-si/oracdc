# oracdc

[Oracle Database](https://www.oracle.com/database/index.html) [CDC](https://en.wikipedia.org/wiki/Change_data_capture) information transfer to [Apache Kafka](http://kafka.apache.org/), or [Amazon Kinesis](https://aws.amazon.com/kinesis/). **oracdc** materializes Oracle RDBMS materialized view log at heterogeneous database system.
Starting from Oracle RDBMS 12c various Oracle tools for [CDC](https://en.wikipedia.org/wiki/Change_data_capture) and/or replication are [depreciated and desupported](https://docs.oracle.com/database/121/UPGRD/deprecated.htm) and replaced by Oracle Golden Gate.
This project is not intended to be 100% replacement of [expensive](https://www.oracle.com/assets/technology-price-list-070617.pdf) Oracle Golden Gate licenses but may help in many cases when you do not have huge volume of data changes. Project was tested on [Oracle E-Business Suite](https://www.oracle.com/applications/ebusiness/) customer instance for transferring accounting information (mostly GL & XLA tables) to further reporting and analytics in [PostgreSQL](https://www.postgresql.org/) database.
Oracle RDBMS [materialized view log's](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/CREATE-MATERIALIZED-VIEW-LOG.html) are used as source for data changes. No materialized view should consume information from materialized view log's which are used by **oracdc**.
**oracdc** can work as standalone [Apache Kafka](http://kafka.apache.org/), or [Amazon Kinesis](https://aws.amazon.com/kinesis/) producer. For instructions how to install and run **oracdc** in producer mode please read [STANDALONE.md](doc/STANDALONE.md). Also **oracdc** can work as [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect) Source and Sink connector. For instructions how to install and run **oracdc** as [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect) Connector please read [KAFKA-CONNECT.md](doc/KAFKA-CONNECT.md).

## Getting Started

These instructions will get you a copy of the project up and running on any platform with JDK8+ support.

### Prerequisites

Before using **oracdc** please check that required Java8+ is installed with

```
echo "Checking Java version"
java -version
```

### Installing

Build with

```
mvn install
```
Then run supplied `install.sh` passing target installation directory as script parameter. This directory will be mentioned hereinafter as `A2_CDC_HOME`.  For example:

```
install.sh /opt/a2/oracdc
```
This will create following directory structure under `$A2_CDC_HOME`

`bin` - shell scripts 
`conf` - configuration files
`doc` - doc files
`lib` - jar files
`log` - producer's log



### Oracle JDBC drivers
Unfortunately due the binary license there is no public repository with the Oracle JDBC Driver and Oracle UCP. You can copy drivers from Oracle RDBMS server and place them to `$A2_CDC_HOME\lib`

```
cp $ORACLE_HOME/jdbc/lib/ojdbc8.jar $A2_CDC_HOME/lib 
cp $ORACLE_HOME/ucp/lib/ucp.jar $A2_CDC_HOME/lib 
```
or download drivers `ojdbc[JAVA-VERSION].jar` and `ucp.jar` from [JDBC and UCP Downloads page](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)

### Using Oracle Wallet
If you will use	Oracle Wallet for storing Oracle Database credentials you need three more jar files: `oraclepki.jar`, `osdt_core.jar`, and `osdt_cert.jar`

```
cp $ORACLE_HOME/jlib/oraclepki.jar $A2_CDC_HOME/lib
cp $ORACLE_HOME/jlib/osdt_core.jar $A2_CDC_HOME/lib
cp $ORACLE_HOME/jlib/osdt_cert.jar $A2_CDC_HOME/lib
```

or download them from [JDBC and UCP Downloads page](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)
Please refer to [Oracle Database documentation](https://docs.oracle.com/en/database/) for instructions how to create and manage the Oracle Wallet and tnsnames.ora file. Oracle recommends that you create and manage the Wallet in a database environment using `mkstore` command

```
mkstore -wrl $A2_CDC_HOME/wallets/ -create
mkstore -wrl $A2_CDC_HOME/wallets/ -createCredential JDK8 SCOTT
```

## Running 

Create materialized view log's over replicated tables. These materialized view logs _must_ be created _with_ following options - **with primary key**, _and/or_ **with rowid**, **sequence**, **excluding new values** and _without_ **commit scn** option. You do not need to specify _column list_ while creating materialized view log for using with **oracdc**. **oracdc** reads from materialized view log only primary key value and/or rowid of row in master table.  Table below describes how **oracdc** operates depending on the materialized view log settings

|SNAPSHOT LOG WITH    |Master table access|Key for External system            |
|:--------------------|:------------------|:----------------------------------|
|Primary key          |Primary key        |Primary key                        |
|Primary key and ROWID|ROWID              |Primary key                        |
|ROWID                |ROWID              |ROWID (String, key name ORA$ROW$ID)|


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
If using **oracdc** in standalone mode start it using supplied *oracdc-producer.sh*, or follow **oracdc** [documentation for Apache Kafka Connect](doc/KAFKA-CONNECT.md)] mode for information how to start Source Connector. Execute at Oracle RDBMS side INSERT/UPDATE/DELETE.
If running with Kafka check for audit information at [Kafka](http://kafka.apache.org/)'s side with command line consumer

```
bin/kafka-console-consumer.sh --from-beginning --zookeeper localhost:2181 --topic oracdc-topic
```
If running with [Amazon Kinesis](https://aws.amazon.com/kinesis/) check for transferred audit files with [aws kinesis get-records](https://docs.aws.amazon.com/cli/latest/reference/kinesis/get-records.html) CLI command.

## Message format

Every message is envelope has two parts: a *schema* and *payload*. The schema describes the structure of the payload, while the payload contains the actual data. Message format is similar to format used by [Debezium](https://debezium.io/) with two important differences:
1) *before* part of *schema* and *payload* used only for **DELETE** operation
2) for **DELETE** operation *schema* and *payload* contain only primary key definition and value due to limit of information from materialized view log
3) for **INSERT** and **UPDATE** only *after* part of payload contain values due to absense of row previous state in materialized view log's

### Message examples
See the [MESSAGE-SAMPLES-DBZM-STYLE.md](doc/MESSAGE-SAMPLES-DBZM-STYLE.md) for details

## Oracle RDBMS specific information
*source* structure of payload contains various information about Oracle RDBMS instance from V$INSTANCE and V$DATABASE views.
*scn* field for **INSERT** and **UPDATE** operations contains actual *ORA_ROWSCN* pseudocolumn value for given row in master table, for **DELETE** operation -  *ORA_ROWSCN* pseudocolumn value for given row in materialized view log.

### Oracle RDBMS Type mapping

|Oracle RDBMS Type|JSON Type|Comment                                                       |
|:----------------|:--------|:-------------------------------------------------------------|
|DATE             |int32    |org.apache.kafka.connect.data.Date                            |
|TIMESTAMP        |int64    |org.apache.kafka.connect.data.Timestamp                       |
|NUMBER           |int8     |NUMBER(1,0) & NUMBER(2,0)                                     |
|NUMBER           |int16    |NUMBER(3,0) & NUMBER(4,0)                                     |
|NUMBER           |int32    |NUMBER(5,0) & NUMBER(6,0) & NUMBER(7,0) & NUMBER(8,0)         |
|NUMBER           |int64    |Other Integers between billion and 1,000,000,000,000,000,000  |
|NUMBER           |float64  |Oracle NUMBER without specified SCALE and PRECISION           |
|NUMBER           |bytes    |org.apache.kafka.connect.data.Decimal - all other numerics    |
|FLOAT            |float64  |                                                              |
|RAW              |bytes    |                                                              |
|BLOB             |bytes    |                                                              |
|CHAR             |string   |                                                              |
|NCHAR            |string   |                                                              |
|VARCHAR2         |string   |                                                              |
|NVARCHAR2        |string   |                                                              |

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## TODO
* Oracle LOB handler: convert Oracle BLOB/CLOB/BFILE to link on object file system and send ref to instead of large data
* [Oracle Log Miner](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sutil/oracle-logminer-utility.html) or [Oracle Flashback](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/adfns/flashback.html) as CDC source
* [Apache Zookeeper](http://zookeeper.apache.org/) for HA and dynamic configuration
* AWS Lambda/AWS ECS Consumer

## Version history
#####0.9.0 (OCT-2019)
Initial release
#####0.9.1 (NOV-2019)
Oracle Wallet support for storing database credentials
#####0.9.2 (DEC-2019)
"with ROWID" materialized view log support

## Authors

* **Aleksej Veremeev** - *Initial work* - [A2 Rešitve d.o.o.](http://a2-solutions.eu/)

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details

