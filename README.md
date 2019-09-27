# oracdc

[Oracle Database](https://www.oracle.com/database/index.html) [CDC](https://en.wikipedia.org/wiki/Change_data_capture) information transfer to [Apache Kafka](http://kafka.apache.org/) or [Amazon Kinesis](https://aws.amazon.com/kinesis/).
Starting from Oracle RDBMS 12c various Oracle tools for [CDC](https://en.wikipedia.org/wiki/Change_data_capture) and/or replication are [depreciated and desupported](https://docs.oracle.com/database/121/UPGRD/deprecated.htm) and replaced by Oracle Golden Gate.
This project is not intended to be 100% replacement of [expensive](https://www.oracle.com/assets/technology-price-list-070617.pdf) Oracle Golden Gate licenses but may help in many cases when you do not have huge volume of data changes. Project was tested on [Oracle E-Business Suite](https://www.oracle.com/applications/ebusiness/) customer instance for transferring accounting information (mostly GL & XLA tables) to further reporting and analytics in [PostgreSQL](https://www.postgresql.org/) database.
Oracle RDBMS [materialized view log's](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/CREATE-MATERIALIZED-VIEW-LOG.html) are used as source for data changes. No materialized view should consume information from materialized view log's which are used by **oracdc**.

## Getting Started

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
Then run as root supplied `install.sh` or run commands below

```
ORACDC_HOME=/opt/a2/agents/oracdc

mkdir -p $ORACDC_HOME/lib
cp target/lib/*.jar $ORACDC_HOME/lib

cp target/oracdc-kafka-0.9.0.jar $ORACDC
cp oracdc-producer.sh $ORACDC
cp oracdc-producer.conf $ORACDC
cp log4j.properties $ORACDC

chmod +x $ORACDC_HOME/oracdc-producer.sh
```
Unfortunately due the binary license there is no public repository with the Oracle JDBC Driver and Oracle UCP. You can copy drivers from Oracle RDBMS server and place they to **oracdc** lib directory

```
ORACDC_HOME=/opt/a2/agents/oracdc
cp $ORACLE_HOME/jdbc/lib/ojdbc8.jar $ORACDC_HOME/lib 
cp $ORACLE_HOME/ucp/lib/ucp.jar $ORACDC_HOME/lib 
```
or download drivers from [https://www.oracle.com/database/technologies/jdbc-ucp-122-downloads.html](https://www.oracle.com/database/technologies/jdbc-ucp-122-downloads.html)

## Configuration
### Mandatory parameters
`a2.jdbc.url` - JDBC connection URL

`a2.jdbc.username` - JDBC connection username

`a2.jdbc.password` - JDBC connection password

### Optional parameters
`a2.target.broker` - Type of broker/stream used (**kafka**/**kinesis**). Set to **kafka** by default, for Amazon Kinesis must set to **kinesis**

`a2.poll.interval` - interval in milliseconds to poll for new data in each materialized view log

`a2.batch.size` - maximum number of rows to include in a single batch when polling for new data

`a2.exclude` - comma separated list of tables to exclude from **oracdc** processing

### Apache Kafka specific configuration

Create topic using command line interface, for example: 

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ora-cdc-topic
```

Don't forget about correct sizing of topic for heavy load.
If you using Amazon Managed Streaming for Apache Kafka you can use [AWS Management Console](https://console.aws.amazon.com/msk) 

#### Mandatory parameters for Apache Kafka

`a2.kafka.servers` - hostname/IP address and port of Kafka installation

`a2.kafka.topic` - value must match name of Kafka topic created on previous step

`a2.kafka.client.id` - use any valid string value for identifying this Kafka producer


#### Optional parameters for Apache Kafka
`a2.kafka.security.protocol` - must be set to `SSL` or `SASL_SSL` if you like to transmit files using SSL and enable auth. Only PLAIN authentication supported and tested at moment.

`a2.kafka.security.truststore.location` - set to valid certificate store file if `a2.security.protocol` set to `SSL` or `SASL_SSL`

`a2.kafka.security.truststore.password` - password for certificate store file if `a2.security.protocol` set to `SSL` or `SASL_SSL`

`a2.kafka.security.jaas.config` - JAAS login module configuration. Must be set when `a2.security.protocol` set to `SASL_SSL`. For example **org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="alice-secret";** . Do not forget to escape equal sign and double quotes in file.

`a2.kafka.acks` - number of acknowledgments. Please check [Apache Kafka documentation](https://kafka.apache.org/documentation/#configuration) for more information about `acks` parameter
 
`a2.kafka.batch.size` - producer batch size. Please check [Apache Kafka documentation](https://kafka.apache.org/documentation/#configuration) for more information about `batch.size` parameter 

`a2.kafka.buffer.memory` - producer buffer memory. Please check [Apache Kafka documentation](https://kafka.apache.org/documentation/#configuration) for more information about `buffer.memory` parameter 

`a2.kafka.compression.type` - compression type. Please check [Apache Kafka documentation](https://kafka.apache.org/documentation/#configuration) for more information about `compression.type` parameter. By default set to `gzip`, to disable compression set to `uncompressed` 

`a2.kafka.linger.ms` - producer linger time. Please check [Apache Kafka documentation](https://kafka.apache.org/documentation/#configuration) for more information about `linger.ms` parameter

`a2.kafka.max.request.size` - maximum size of producer producer request. Please check [Apache Kafka documentation](https://kafka.apache.org/documentation/#configuration) for more information about `max.request.size` parameter

`a2.kafka.retries` - producer retries config. Please check [Apache Kafka documentation](https://kafka.apache.org/documentation/#configuration) for more information about `retries` parameter

#### Example parameter file for Apache Kafka
Parameter file for Apache Kafka `oracdc-producer.conf` should looks like

```
a2.jdbc.url = jdbc:oracle:thin:@//ora122.a2-solutions.eu:1521/JDK8
a2.username = scott
a2.password = tiger

a2.poll.interval = 5000
a2.batch.size = 100
a2.exclude = CUSTOMERS, ORDERS

a2.kafka.servers = kafka.a2-solutions.eu:9092
a2.kafka.topic = ora-cdc-topic
a2.kafka.client.id = a2.oracdc
a2.kafka.compression.type = none

```

### Configuration for Amazon Kinesis 
Create Kinesis stream using [AWS Management Console](https://console.aws.amazon.com/kinesis) or using AWS CLI, for example:

```
aws kinesis create-stream --stream-name ora-aud-test --shard-count 1
```
Check stream's creation progress using [AWS Management Console](https://console.aws.amazon.com/kinesis) or with AWS CLI, for example:

```
aws kinesis describe-stream --stream-name ora-aud-test
```
Don't forget about correct sizing of stream for heavy load.

#### Mandatory parameters for Amazon Kinesis

`a2.kinesis.region` - AWS region

`a2.kinesis.stream` - name of Kinesis stream

`a2.kinesis.access.key` - AWS access key

`a2.kinesis.access.secret` - AWS access key secret

#### Optional parameters for Amazon Kinesis

`a2.kinesis.max.connections` - can be used to control the degree of parallelism when making HTTP requests. Using a high number will cause a bunch of broken pipe errors to show up in the logs. This is due to idle connections being closed by the server. Setting this value too large may also cause request timeouts if you do not have enough bandwidth. **1** is default value

`a2.kinesis.request.timeout` - Request timeout milliseconds. **30000** is default value

`a2.kinesis.request.record.max.buffered.time` - controls how long records are allowed to wait  in the Kinesis Producer's buffers before being sent. Larger values increase aggregation and reduces the number of Kinesis records put, which can be helpful if you're getting throttled because of the records per second limit on a shard.. **5000** is default value

`a2.kinesis.file.size.threshold` - Maximum size of audit file transferred without compression. **512** is default value

#### Example parameter file for Amazon Kinesis
Parameter file for Amazon Kinesis `oracdc-producer.conf` should looks like

```
a2.jdbc.url = jdbc:oracle:thin:@//ora122.a2-solutions.eu:1521/JDK8
a2.username = scott
a2.password = tiger

a2.poll.interval = 5000
a2.batch.size = 100
a2.exclude = CUSTOMERS, ORDERS

a2.target.broker = kinesis

a2.kinesis.region = eu-west-1
a2.kinesis.stream = ora-cdc-test
a2.kinesis.access.key = AAAAAAAAAABBBBBBBBBB
a2.kinesis.access.secret = AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDD
```


## Running 

Create materialed view log's over replicated tables. These materialized view logs should created with following options - **with primary key**, **sequence**, **excluding new values** and without **commit scn** option

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
Start **oracdc** and execute INSERT/UPDATE/DELETE. 
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

### Oracle specific information
*source* structure of payload contains various information about Oracle RDBMS instance from V$INSTANCE and V$DATABASE views.
*scn* field for **INSERT** and **UPDATE** operations contains actual *ORA_ROWSCN* pseudocolumn value for given row in master table, for **DELETE** operation -  *ORA_ROWSCN* pseudocolumn value for given row in materialized view log.

### 'c' - INSERT message example

```
{
  "schema" : {
    "type" : "struct",
    "optional" : false,
    "name" : "SCOTT.DEPT.Envelope",
    "fields" : [ {
      "type" : "struct",
      "optional" : true,
      "field" : "before",
      "name" : "SCOTT.DEPT.PK",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      } ]
    }, {
      "type" : "struct",
      "optional" : true,
      "field" : "after",
      "name" : "SCOTT.DEPT.Data",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "DNAME"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "LOC"
      } ]
    }, {
      "type" : "struct",
      "optional" : false,
      "field" : "source",
      "name" : "eu.solutions.a2.cdc.oracle",
      "fields" : [ {
        "type" : "int64",
        "optional" : false,
        "field" : "dbid"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "database_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "platform_name"
      }, {
        "type" : "int16",
        "optional" : false,
        "field" : "instance_number"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "instance_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "host_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "version"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "ts_ms"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "owner"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "table"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "scn"
      } ]
    }, {
      "type" : "string",
      "optional" : false,
      "field" : "op"
    }, {
      "type" : "int64",
      "optional" : false,
      "field" : "ts_ms"
    } ]
  },
  "payload" : {
    "before" : null,
    "after" : {
      "DEPTNO" : 10,
      "DNAME" : "ACCOUNTING",
      "LOC" : "NEW YORK"
    },
    "source" : {
      "ts_ms" : 1569178592502,
      "owner" : "SCOTT",
      "table" : "DEPT",
      "scn" : 2288632,
      "dbid" : 3346093197,
      "database_name" : "JDK8",
      "platform_name" : "Linux x86 64-bit",
      "instance_number" : 1,
      "instance_name" : "JDK8",
      "host_name" : "kafka.wine-gu.ru",
      "version" : "12.2.0.1.0"
    },
    "op" : "c",
    "ts_ms" : 1569178592503
  }
}
```
### 'u' - UPDATE message example

```
{
  "schema" : {
    "type" : "struct",
    "optional" : false,
    "name" : "SCOTT.DEPT.Envelope",
    "fields" : [ {
      "type" : "struct",
      "optional" : true,
      "field" : "before",
      "name" : "SCOTT.DEPT.PK",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      } ]
    }, {
      "type" : "struct",
      "optional" : true,
      "field" : "after",
      "name" : "SCOTT.DEPT.Data",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "DNAME"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "LOC"
      } ]
    }, {
      "type" : "struct",
      "optional" : false,
      "field" : "source",
      "name" : "eu.solutions.a2.cdc.oracle",
      "fields" : [ {
        "type" : "int64",
        "optional" : false,
        "field" : "dbid"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "database_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "platform_name"
      }, {
        "type" : "int16",
        "optional" : false,
        "field" : "instance_number"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "instance_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "host_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "version"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "ts_ms"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "owner"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "table"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "scn"
      } ]
    }, {
      "type" : "string",
      "optional" : false,
      "field" : "op"
    }, {
      "type" : "int64",
      "optional" : false,
      "field" : "ts_ms"
    } ]
  },
  "payload" : {
    "before" : null,
    "after" : {
      "DEPTNO" : 20,
      "DNAME" : "RESEARCH",
      "LOC" : "DALLAS"
    },
    "source" : {
      "ts_ms" : 1569178592721,
      "owner" : "SCOTT",
      "table" : "DEPT",
      "scn" : 2288632,
      "dbid" : 3346093197,
      "database_name" : "JDK8",
      "platform_name" : "Linux x86 64-bit",
      "instance_number" : 1,
      "instance_name" : "JDK8",
      "host_name" : "kafka.wine-gu.ru",
      "version" : "12.2.0.1.0"
    },
    "op" : "u",
    "ts_ms" : 1569178592722
  }
}
```
### 'd' - DELETE message example

```
{
  "schema" : {
    "type" : "struct",
    "optional" : false,
    "name" : "SCOTT.DEPT.Envelope",
    "fields" : [ {
      "type" : "struct",
      "optional" : true,
      "field" : "before",
      "name" : "SCOTT.DEPT.PK",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      } ]
    }, {
      "type" : "struct",
      "optional" : true,
      "field" : "after",
      "name" : "SCOTT.DEPT.Data",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "DNAME"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "LOC"
      } ]
    }, {
      "type" : "struct",
      "optional" : false,
      "field" : "source",
      "name" : "eu.solutions.a2.cdc.oracle",
      "fields" : [ {
        "type" : "int64",
        "optional" : false,
        "field" : "dbid"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "database_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "platform_name"
      }, {
        "type" : "int16",
        "optional" : false,
        "field" : "instance_number"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "instance_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "host_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "version"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "ts_ms"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "owner"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "table"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "scn"
      } ]
    }, {
      "type" : "string",
      "optional" : false,
      "field" : "op"
    }, {
      "type" : "int64",
      "optional" : false,
      "field" : "ts_ms"
    } ]
  },
  "payload" : {
    "before" : {
      "DEPTNO" : 30
    },
    "after" : null,
    "source" : {
      "ts_ms" : 1569178592500,
      "owner" : "SCOTT",
      "table" : "DEPT",
      "scn" : 2991403,
      "dbid" : 3346093197,
      "database_name" : "JDK8",
      "platform_name" : "Linux x86 64-bit",
      "instance_number" : 1,
      "instance_name" : "JDK8",
      "host_name" : "kafka.wine-gu.ru",
      "version" : "12.2.0.1.0"
    },
    "op" : "d",
    "ts_ms" : 1569178592730
  }
}
```
## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## TODO
* [Kafka Connect](http://kafka.apache.org/documentation/#connect) mode
* [Oracle Log Miner](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sutil/oracle-logminer-utility.html) or [Oracle Flashback](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/adfns/flashback.html) as CDC source
* [Apache Zookeeper](http://zookeeper.apache.org/) for HA and dynamic configuration
* AWS Lambda/AWS ECS Consumer

## Authors

* **Aleksej Veremeev** - *Initial work* - [A2 Rešitve d.o.o.](http://a2-solutions.eu/)

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details

