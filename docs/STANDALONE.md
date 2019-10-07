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

