# oracdc

[Oracle Database](https://www.oracle.com/database/index.html) [CDC](https://en.wikipedia.org/wiki/Change_data_capture) information transfer to [Apache Kafka](http://kafka.apache.org/), or [Amazon Kinesis](https://aws.amazon.com/kinesis/). **oracdc** materializes Oracle RDBMS materialized view log at heterogeneous database system.
Starting from Oracle RDBMS 12c various Oracle tools for [CDC](https://en.wikipedia.org/wiki/Change_data_capture) and/or replication are [depreciated and desupported](https://docs.oracle.com/database/121/UPGRD/deprecated.htm) and replaced by Oracle Golden Gate.
This project is not intended to be 100% replacement of [expensive](https://www.oracle.com/assets/technology-price-list-070617.pdf) Oracle Golden Gate licenses but may help in many cases when you do not have huge volume of data changes. Project was tested on [Oracle E-Business Suite](https://www.oracle.com/applications/ebusiness/) customer instance for transferring accounting information (mostly GL & XLA tables) to further reporting and analytics in [PostgreSQL](https://www.postgresql.org/) database.
Oracle RDBMS [materialized view log's](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/CREATE-MATERIALIZED-VIEW-LOG.html) are used as source for data changes. No materialized view should consume information from materialized view log's which are used by **oracdc**.
**oracdc** can work as standalone [Apache Kafka](http://kafka.apache.org/), or [Amazon Kinesis](https://aws.amazon.com/kinesis/) producer. For instructions how to install and run **oracdc** in producer mode please read [STANDALONE.md](docs/STANDALONE.md). Also **oracdc** can work as [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect) Source and Sink connector. For instructions how to install and run **oracdc** as [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect) Connector please read [KAFKA-CONNECT.md](docs/KAFKA-CONNECT.md).


## Running 

Create materialized view log's over replicated tables. These materialized view logs _must_ be created _with_ following options - **with primary key**, **sequence**, **excluding new values** and _without_ **commit scn** option

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
If using **oracdc** in standalone mode start it using supplied *oracdc-producer.sh*, or follow **oracdc** [documentation for Apache Kafka Connect](docs/KAFKA-CONNECT.md)] mode for information how to start Source Connector. Execute at Oracle RDBMS side INSERT/UPDATE/DELETE.
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
See the [MESSAGE-SAMPLES-DBZM-STYLE.md](docs/MESSAGE-SAMPLES-DBZM-STYLE.md) for details

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

## Authors

* **Aleksej Veremeev** - *Initial work* - [A2 Rešitve d.o.o.](http://a2-solutions.eu/)

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details

