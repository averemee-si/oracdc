## Configuration
### Mandatory parameters
`a2.jdbc.url` - JDBC connection URL

`a2.jdbc.username` - JDBC connection username

`a2.jdbc.password` - JDBC connection password

`a2.wallet.location` - Location of Oracle Wallet. Not required when `a2.jdbc.url` & `a2.jdbc.username` & `a2.jdbc.password` are set

`a2.tns.admin` - Location of tnsnames.ora file. Not required when `a2.jdbc.url` & `a2.jdbc.username` & `a2.jdbc.password` are set

`a2.tns.alias` - Connection TNS alias. Not required when `a2.jdbc.url` & `a2.jdbc.username` & `a2.jdbc.password` are set

### Optional parameters
`a2.target.broker` - Type of broker/stream used (**kafka**/**kinesis**). Set to **kafka** by default, for Amazon Kinesis must set to **kinesis**

`a2.poll.interval` - interval in milliseconds to poll for new data in each materialized view log

`a2.batch.size` - maximum number of rows to include in a single batch when polling for new data

`a2.exclude` - comma separated list of table names or table names with schema name (**<SCHEMA_NAME>.<TABLE_NAME>**) to exclude from **oracdc** processing. To exclude all schema objects from **oracdc** processing use __<SCHEMA_NAME>.*__ or __<SCHEMA_NAME>.%__ 

`a2.include` - comma separated list of table names or table names with schema name (**<SCHEMA_NAME>.<TABLE_NAME>**) to include to **oracdc** processing. To include all schema objects to **oracdc** processing use __<SCHEMA_NAME>.*__ or __<SCHEMA_NAME>.%__

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

`a2.kinesis.max.connections` - can be used to control the degree of parallelism when making HTTP requests. Using a high number will cause a bunch of broken pipe errors to show up in the logs. This is due to idle connections being closed by the server. Setting this value too large may also cause request timeouts if you do not have enough bandwidth. **50** is default value

`a2.kinesis.request.timeout` - Request timeout milliseconds. **30000** is default value

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

