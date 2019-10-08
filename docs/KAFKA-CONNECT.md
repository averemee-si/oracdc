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
Create **oracdc-connect-standalone.properties** file to configure a standalone worker

```
bootstrap.servers=<KAFKA BROKER HOST>:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true

offset.storage.file.filename=/<KAFKA DATA PATH>/oracdc.connect.offsets
offset.flush.interval.ms=10000

plugin.path=<PATH TO ORACDC jar file and required JDBC drivers>
```
Create **oracdc-source-connector.properties** to configure Oracle RDBMS source

```
name=oracdc-oebs-source
connector.class=eu.solutions.a2.cdc.oracle.OraCdcSourceConnector
tasks.max=3

a2.kafka.topic=oracdc-topic
a2.jdbc.url=jdbc:oracle:thin:@//ebsdb.a2-solutions.eu:1521/EBSDB
a2.jdbc.username=apps
a2.jdbc.password=apps

```
**N.B.** **tasks.max** _must_ be set to number of materialized view logs for processing with **oracdc**

Create **oracdc-sink-connector.properties** to configure target database (currently tested with MySQL/MariaDB & PostgreSQL)

```
name=oracdc-oebs-sink
connector.class=eu.solutions.a2.cdc.oracle.OraCdcJdbcSinkConnector
tasks.max=1

# The topics to consume from - required for sink connectors like this one
topics=oracdc-topic

a2.jdbc.url=jdbc:mysql://polyxena.a2-solutions.eu:3306/EBSDB
a2.jdbc.username=apps
a2.jdbc.password=apps
a2.autocreate=true

```
### 3rd party JDBC drivers
Unfortunately due the binary license there is no public repository with the Oracle JDBC Driver and Oracle UCP. You can copy drivers from Oracle RDBMS server and place they to **plugin.path** connector directory

```
cp $ORACLE_HOME/jdbc/lib/ojdbc8.jar $KAFKA_CONNECT_PLUGIN_PATH 
cp $ORACLE_HOME/ucp/lib/ucp.jar $KAFKA_CONNECT_PLUGIN_PATH 
```
or download drivers from [https://www.oracle.com/database/technologies/jdbc-ucp-122-downloads.html](https://www.oracle.com/database/technologies/jdbc-ucp-122-downloads.html)

Also you need to copy to _KAFKA_CONNECT_PLUGIN_PATH_ required target database JDBC drivers and HikariCP-3.4.1.jar.

### Starting
Start **oracdc** with

```
$KAFKA_HOME/bin/connect-standalone.sh \
oracdc-source-connector.properties \
oracdc-source-connector.properties \
oracdc-sink-connector.properties
```

## Configuration
### Mandatory parameters
`a2.jdbc.url` - JDBC connection URL
`a2.jdbc.username` - JDBC connection username
`a2.jdbc.password` - JDBC connection password
`a2.kafka.topic` - _Source Connector_ only: topic to send data

### Optional parameters

`a2.batch.size` - default _1000_, maximum number of rows to include in a single batch when polling for new data in _Source Connector_ or  consuming in _Sink Connector_
`a2.poll.interval` - _Source Connector_ only: interval in milliseconds to poll for new data in each materialized view log
`a2.exclude` - _Source Connector_ only: comma separated list of tables to exclude from **oracdc** processing
`a2.autocreate`- _Sink Connector_ only: default _false_, when set to true **oracdc** creates missing table automatically
