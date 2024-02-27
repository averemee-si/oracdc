# Copyright (c) 2018-present, A2 Rešitve d.o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
# the License for the specific language governing permissions and limitations under the License.
FROM   amazoncorretto:17-alpine-jdk
LABEL  maintainer="oracle@a2-solutions.eu"
LABEL  vendor="A2 Rešitve d.o.o."
LABEL  version="2.2.0"
LABEL  release="2.2.0"
LABEL  name="oracdc: Oracle RDBMS CDC and data streaming"
LABEL  summary="oracdc and all dependencies for optimal work. When started, it will run the Kafka Connect framework in distributed mode."

RUN    apk add --no-cache gcompat tzdata bash
RUN    addgroup -S kafka && adduser -S kafka -G kafka
ARG    BASEDIR=/opt

ARG    KAFKA_VERSION=3.6.1
ARG    SCALA_VERSION=2.13
ARG    KAFKA_FILENAME=kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
ENV    KAFKA_HOME=${BASEDIR}/kafka
ENV    PATH=${PATH}:${KAFKA_HOME}/bin:${KAFKA_HOME}/connect/bin
ENV    PROPS_FILE=${KAFKA_HOME}/config/oracdc-distributed.properties
ENV    KAFKA_OPTS="-Dchronicle.analytics.disable=true -Dchronicle.disk.monitor.disable=true --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
RUN    wget "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_FILENAME}" -O "/tmp/${KAFKA_FILENAME}" \
       && tar xvfz /tmp/${KAFKA_FILENAME} -C ${BASEDIR} \
       && rm /tmp/${KAFKA_FILENAME} \
       && ln -s ${BASEDIR}/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME} \
       && mkdir -p ${KAFKA_HOME}/connect/lib \
       && mkdir -p ${KAFKA_HOME}/connect/bin \
       && mkdir -p ${KAFKA_HOME}/connect/jmx \
       && chown -R kafka:kafka ${BASEDIR}/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME}

ARG    ORACDC_VERSION=2.2.0
ARG    ORACDC_FILENAME=oracdc-kafka-${ORACDC_VERSION}-standalone.jar
COPY   target/${ORACDC_FILENAME} ${KAFKA_HOME}/connect/lib
RUN    chown -R kafka:kafka ${KAFKA_HOME}/connect/lib/${ORACDC_FILENAME}

ENV    ORACDC_JAR=${KAFKA_HOME}/connect/lib/${ORACDC_FILENAME}
ARG    MAIN_CONFIG=solutions.a2.cdc.oracle.utils.file.Env2Property
RUN    mkdir ${BASEDIR}/oracdc \
       && touch ${BASEDIR}/oracdc/run.sh \
       && chmod +x ${BASEDIR}/oracdc/run.sh \
       && echo "#!/bin/sh" > ${BASEDIR}/oracdc/run.sh \
       && echo "java -cp ${ORACDC_JAR} ${MAIN_CONFIG} A2_ORACDC_ ${PROPS_FILE} --append" >> ${BASEDIR}/oracdc/run.sh \
       && echo "cat ${PROPS_FILE}" >> ${BASEDIR}/oracdc/run.sh \
       && echo "connect-distributed.sh $PROPS_FILE" >> ${BASEDIR}/oracdc/run.sh \
       && chown -R kafka:kafka ${BASEDIR}/oracdc

USER   kafka
RUN    mkdir ${KAFKA_HOME}/logs
RUN    touch ${KAFKA_HOME}/logs/connect.log

RUN    echo "" > ${PROPS_FILE} \
       && echo "offset.flush.interval.ms=10000" >> ${PROPS_FILE} \
       && echo "offset.flush.timeout.ms=5000" >> ${PROPS_FILE} \
       && echo "internal.key.converter=org.apache.kafka.connect.json.JsonConverter" >> ${PROPS_FILE} \
       && echo "internal.value.converter=org.apache.kafka.connect.json.JsonConverter" >> ${PROPS_FILE} \
       && echo "internal.key.converter.schemas.enable=false" >> ${PROPS_FILE} \
       && echo "internal.value.converter.schemas.enable=false" >> ${PROPS_FILE} \
       && echo "key.converter=org.apache.kafka.connect.json.JsonConverter" >> ${PROPS_FILE} \
       && echo "value.converter=org.apache.kafka.connect.json.JsonConverter" >> ${PROPS_FILE} \
       && echo "key.converter.schemas.enable=true" >> ${PROPS_FILE} \
       && echo "value.converter.schemas.enable=true" >> ${PROPS_FILE} \
       && echo "plugin.path=/opt/kafka/connect/lib" >> ${PROPS_FILE}

# Add schema registry dependencies
ARG    CONFLUENT_VERSION=7.3.7
ARG    AVRO_VERSION=1.11.3
ARG    C_COMPRESS_VERSION=1.26.0
ARG    GUAVA_VERSION=33.0.0-jre
ARG    FA_VERSION=1.0.2
RUN    WORKDIR=/tmp/$RANDOM && mkdir -p $WORKDIR && cd $WORKDIR \
       && wget "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/${CONFLUENT_VERSION}/kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar" \
             -O "kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar" \
       && jar xvf "kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar" && rm -f "kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar" \
       && wget "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/${CONFLUENT_VERSION}/kafka-connect-avro-data-${CONFLUENT_VERSION}.jar" \
             -O "kafka-connect-avro-data-${CONFLUENT_VERSION}.jar" \
       && jar xvf "kafka-connect-avro-data-${CONFLUENT_VERSION}.jar" && rm -f "kafka-connect-avro-data-${CONFLUENT_VERSION}.jar" \
       && wget "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/${CONFLUENT_VERSION}/kafka-avro-serializer-${CONFLUENT_VERSION}.jar" \
             -O "kafka-avro-serializer-${CONFLUENT_VERSION}.jar" \
       && jar xvf "kafka-avro-serializer-${CONFLUENT_VERSION}.jar" && rm -f "kafka-avro-serializer-${CONFLUENT_VERSION}.jar" \
       && wget "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/${CONFLUENT_VERSION}/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" \
             -O "kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" \
       && jar xvf "kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" && rm -f "kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" \
       && wget "https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/${CONFLUENT_VERSION}/kafka-schema-serializer-${CONFLUENT_VERSION}.jar" \
             -O "kafka-schema-serializer-${CONFLUENT_VERSION}.jar" \
       && jar xvf "kafka-schema-serializer-${CONFLUENT_VERSION}.jar" && rm -f "kafka-schema-serializer-${CONFLUENT_VERSION}.jar" \
       && wget "https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/${CONFLUENT_VERSION}/kafka-schema-converter-${CONFLUENT_VERSION}.jar" \
             -O "kafka-schema-converter-${CONFLUENT_VERSION}.jar" \
       && jar xvf "kafka-schema-converter-${CONFLUENT_VERSION}.jar" && rm -f "kafka-schema-converter-${CONFLUENT_VERSION}.jar" \
       && wget "https://repo1.maven.org/maven2/org/apache/avro/avro/${AVRO_VERSION}/avro-${AVRO_VERSION}.jar" \
             -O "avro-${AVRO_VERSION}.jar" \
       && jar xvf "avro-${AVRO_VERSION}.jar" && rm -f "avro-${AVRO_VERSION}.jar" \
       && wget "https://repo1.maven.org/maven2/org/apache/commons/commons-compress/${C_COMPRESS_VERSION}/commons-compress-${C_COMPRESS_VERSION}.jar" \
             -O "commons-compress-${C_COMPRESS_VERSION}.jar" \
       && jar xvf "commons-compress-${C_COMPRESS_VERSION}.jar" && rm -f "commons-compress-${C_COMPRESS_VERSION}.jar" \
       && wget "https://repo1.maven.org/maven2/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar" \
             -O "guava-${GUAVA_VERSION}.jar" \
       && jar xvf "guava-${GUAVA_VERSION}.jar" && rm -f "guava-${GUAVA_VERSION}.jar" \
       && wget "https://repo1.maven.org/maven2/com/google/guava/failureaccess/${FA_VERSION}/failureaccess-${FA_VERSION}.jar" \
             -O "failureaccess-${FA_VERSION}.jar" \
       && jar xvf "failureaccess-${FA_VERSION}.jar"; rm -f "failureaccess-${FA_VERSION}.jar" \
       && jar cvf "confluent-avro-schema-client-${CONFLUENT_VERSION}.jar" META-INF com io org \
       && mv "confluent-avro-schema-client-${CONFLUENT_VERSION}.jar" ${KAFKA_HOME}/libs \
       && cd ${KAFKA_HOME} && rm -rf WORKDIR

ARG    CHECKER_CMD=${KAFKA_HOME}/connect/bin/oraCheck.sh
RUN    echo "#! /bin/sh" > ${CHECKER_CMD} \
       && echo "java -cp \${ORACDC_JAR} solutions.a2.cdc.oracle.utils.OracleSetupCheck \"\$@\"" >> ${CHECKER_CMD} \
       && chmod +x ${CHECKER_CMD}

ARG    EXPORTER_VERSION=0.20.0
ARG    EXPORTER_FILE=jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar
RUN    cd ${KAFKA_HOME}/connect/jmx \
RUN    && wget "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/${EXPORTER_FILE}" \
          -O ${EXPORTER_FILE}

ARG    EXPORTER_CONFIG=${KAFKA_HOME}/connect/jmx/connect.yaml
RUN    echo "---" > ${EXPORTER_CONFIG} \
       && echo "rules:"  >> ${EXPORTER_CONFIG} \
       && echo "- pattern: \".*\"" >> ${EXPORTER_CONFIG}

ENV    KAFKA_JMX_OPTS="-javaagent:${KAFKA_HOME}/connect/jmx/${EXPORTER_FILE}=9083:${EXPORTER_CONFIG} ${KAFKA_JMX_OPTS}"

# Prometeus JMX Exporter
EXPOSE 9083
# Default kafka-connect rest.port
EXPOSE 8083
VOLUME ["/var/lib/oracdc"]
CMD    ["/opt/oracdc/run.sh"]

