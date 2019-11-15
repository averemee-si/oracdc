## Configuration
### Mandatory parameters
`a2.jdbc.url` - JDBC connection URL

`a2.jdbc.username` - JDBC connection username

`a2.jdbc.password` - JDBC connection password

`a2.schema.type` - _Source Connector_ only: default _kafka_. This parameter tells **oracdc** which schema use, and which key & value converters use.
When set to _kafka_ **oracdc** must be configured to use  _org.apache.kafka.connect.json.JsonConverter_ and produces pure Kafka Connect schema compatible with [Confluent JDBC Sink Connector](https://docs.confluent.io/3.2.0/connect/connect-jdbc/docs/sink_connector.html).
When set to _standalone_  **oracdc** must be configured to use _org.apache.kafka.connect.storage.StringConverter_ and produces [Debezium](https://debezium.io/documentation/reference/0.10/configuration/avro.html) like schema's. Messages in this mode can be consumed with internal **oracdc** sink connector. 

`a2.topic.prefix` - _Source Connector_ only: default _<EMPTYSTRING>_ prefix to prepend table names to generate name of Kafka topic. This parameter is used when **oracdc** configured with `a2.schema.type`=_kafka_ 

`a2.kafka.topic` - _Source Connector_ only: default _oracdc-topic_ topic to send data. This parameter is used when **oracdc** configured with `a2.schema.type`=_standalone_ 


### Optional parameters

`a2.batch.size` - default _1000_, maximum number of rows to include in a single batch when polling for new data in _Source Connector_ or  consuming in _Sink Connector_

`a2.poll.interval` - _Source Connector_ only: interval in milliseconds to poll for new data in each materialized view log

`a2.exclude` - _Source Connector_ only: comma separated list of tables to exclude from **oracdc** processing

`a2.autocreate`- _Sink Connector_ only: default _false_, when set to true **oracdc** creates missing table automatically


### Configuration example (a2.schema.type=kafka)

Create **oracdc-connect-standalone.properties** file to configure a standalone worker

```
bootstrap.servers=<KAFKA BROKER HOST>:9092
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true

offset.storage.file.filename=/<KAFKA DATA PATH>/oracdc.connect.offsets
offset.flush.interval.ms=10000

plugin.path=<PATH TO ORACDC jar file>
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

Create **jdbc-sink-connector.properties** to configure target database (currently tested with MySQL/MariaDB & PostgreSQL)

```
name=jdbc-oebs-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=3

# The topics to consume from - required for sink connectors like this one
topics=GL_CODE_COMBINATIONS,XLA_AE_LINES,XLA_AE_HEADERS

insert.mode=upsert
delete.enabled=true
pk.mode=record_key

connection.url=jdbc:mysql://polyxena.a2-solutions.eu:3306/EBSDB
connection.user=apps
connection.password=apps
auto.create=true

```

### Configuration example (a2.schema.type=standalone)

Create **oracdc-connect-standalone.properties** file to configure a standalone worker

```
bootstrap.servers=<KAFKA BROKER HOST>:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true

offset.storage.file.filename=/<KAFKA DATA PATH>/oracdc.connect.offsets
offset.flush.interval.ms=10000

plugin.path=<PATH TO ORACDC jar file>
```
Create **oracdc-source-connector.properties** to configure Oracle RDBMS source

```
name=oracdc-oebs-source
connector.class=eu.solutions.a2.cdc.oracle.OraCdcSourceConnector
tasks.max=3

a2.schema.type=standalone

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

### Starting
Start **oracdc** with

```
export CLASSPATH=$A2_CDC_HOME/lib/HikariCP-3.4.1.jar:$A2_CDC_HOME/lib/ucp.jar:$A2_CDC_HOME/lib/ojdbc8.jar:$A2_CDC_HOME/lib/oraclepki.jar:$A2_CDC_HOME/lib/osdt_core.jar:$A2_CDC_HOME/lib/osdt_cert.jar
$KAFKA_HOME/bin/connect-standalone.sh \
oracdc-connect-standalone.properties \
oracdc-source-connector.properties \
oracdc-sink-connector.properties
```


