#
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
#

ARG    CONFLUENT_VERSION=8.1.1
ARG    MVN_BASE="https://repo1.maven.org/maven2"

FROM   eclipse-temurin:25-jdk AS build-sr-client
RUN    set -eux && apt-get update && apt-get --yes install wget 

# Add schema registry dependencies
ARG    CONFLUENT_VERSION
ARG    MVN_BASE
ARG    CONFLUENT_BASE="https://packages.confluent.io/maven/io/confluent"
ARG    GUAVA_VERSION=33.5.0-jre
ARG    FA_VERSION=1.0.3
ARG    KOTLIN_VERSION=2.3.0
ARG    PICOCLI_VERSION=4.7.7
ARG    TINK_VERSION=1.20.0
ARG    JSR305_VERSION=3.0.2
ARG    GSON_VERSION=2.13.2
ARG    EPA_VERSION=2.46.0
ARG    ANTLR4_VERSION=4.13.2
ARG    JACKSON_VERSION=2.19.2
ARG    PROTOP_VERSION=4.0.3
ARG    NESSIE_VERSION=0.6.0
ARG    WOODSTOX_VERSION=7.1.1
ARG    JACKSON_PB_VERSION=0.9.18
ARG    JSONATA_VERSION=2.6.2
ARG    HTTP_CLI_VERSION=5.6
ARG    HTTP_CORE_VERSION=5.4

#
# Confluent AVRO support uber-jar
#
ARG    AVRO_VERSION=1.12.1
ARG    C_COMPRESS_VERSION=1.28.0
RUN    WORKDIR=/tmp/$RANDOM && mkdir -p $WORKDIR && cd $WORKDIR \
       && wget -q \
          "${CONFLUENT_BASE}/kafka-connect-avro-converter/${CONFLUENT_VERSION}/kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-connect-avro-data/${CONFLUENT_VERSION}/kafka-connect-avro-data-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-avro-serializer/${CONFLUENT_VERSION}/kafka-avro-serializer-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-registry-client/${CONFLUENT_VERSION}/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-registry-client-encryption/${CONFLUENT_VERSION}/kafka-schema-registry-client-encryption-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-registry-client-encryption-tink/${CONFLUENT_VERSION}/kafka-schema-registry-client-encryption-tink-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/dek-registry-client/${CONFLUENT_VERSION}/dek-registry-client-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-serializer/${CONFLUENT_VERSION}/kafka-schema-serializer-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-converter/${CONFLUENT_VERSION}/kafka-schema-converter-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-rules/${CONFLUENT_VERSION}/kafka-schema-rules-${CONFLUENT_VERSION}.jar" \
          "${MVN_BASE}/info/picocli/picocli/${PICOCLI_VERSION}/picocli-${PICOCLI_VERSION}.jar" \
          "${MVN_BASE}/com/google/crypto/tink/tink/${TINK_VERSION}/tink-${TINK_VERSION}.jar" \
          "${MVN_BASE}/com/google/code/findbugs/jsr305/${JSR305_VERSION}/jsr305-${JSR305_VERSION}.jar" \
          "${MVN_BASE}/com/google/code/gson/gson/${GSON_VERSION}/gson-${GSON_VERSION}.jar" \
          "${MVN_BASE}/com/google/errorprone/error_prone_annotations/${EPA_VERSION}/error_prone_annotations-${EPA_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/failureaccess/${FA_VERSION}/failureaccess-${FA_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-protobuf/${JACKSON_VERSION}/jackson-dataformat-protobuf-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/woodstox/woodstox-core/${WOODSTOX_VERSION}/woodstox-core-${WOODSTOX_VERSION}.jar" \
          "${MVN_BASE}/com/hubspot/jackson/jackson-datatype-protobuf/${JACKSON_PB_VERSION}/jackson-datatype-protobuf-${JACKSON_PB_VERSION}.jar" \
          "${MVN_BASE}/com/ibm/jsonata4java/JSONata4Java/${JSONATA_VERSION}/JSONata4Java-${JSONATA_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/protoparser/${PROTOP_VERSION}/protoparser-${PROTOP_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-core/${NESSIE_VERSION}/cel-core-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-tools/${NESSIE_VERSION}/cel-tools-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-generated-pb/${NESSIE_VERSION}/cel-generated-pb-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-generated-antlr/${NESSIE_VERSION}/cel-generated-antlr-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/antlr/antlr4-runtime/${ANTLR4_VERSION}/antlr4-runtime-${ANTLR4_VERSION}.jar" \
          "${MVN_BASE}/org/apache/avro/avro/${AVRO_VERSION}/avro-${AVRO_VERSION}.jar" \
          "${MVN_BASE}/org/apache/commons/commons-compress/${C_COMPRESS_VERSION}/commons-compress-${C_COMPRESS_VERSION}.jar" \
          "${MVN_BASE}/org/apache/httpcomponents/client5/httpclient5/${HTTP_CLI_VERSION}/httpclient5-${HTTP_CLI_VERSION}.jar" \
          "${MVN_BASE}/org/apache/httpcomponents/core5/httpcore5/${HTTP_CORE_VERSION}/httpcore5-${HTTP_CORE_VERSION}.jar" \
          "${MVN_BASE}/org/apache/httpcomponents/core5/httpcore5-h2/${HTTP_CORE_VERSION}/httpcore5-h2-${HTTP_CORE_VERSION}.jar" \
       && for file in $(ls *.jar); do jar xvf $file; done \
       && rm -f *.jar \
       && jar cvf "confluent-avro-schema-client-${CONFLUENT_VERSION}.jar" [A-Z]* [a-z]* \
       && mv "confluent-avro-schema-client-${CONFLUENT_VERSION}.jar" / \
       && cd / && rm -rf WORKDIR

#
# Confluent PROTOBUF support uber-jar
#
ARG    SQUP_WIRE_VERSION=5.4.0
ARG    SQUP_OKIO_VERSION=3.16.4
ARG    PLNTR_JAVAPOET_VERSION=0.10.0
ARG    SQUP_KOTLINPOET_VERSION=2.2.0
ARG    PROTOBUF_VERSION=4.33.3
ARG    PROTO_COMMON_VERSION=2.63.2
ARG    JB_ANN_VERSION=24.1.0
RUN    WORKDIR=/tmp/$RANDOM && mkdir -p $WORKDIR && cd $WORKDIR \
       && wget -q \
          "${CONFLUENT_BASE}/kafka-connect-protobuf-converter/${CONFLUENT_VERSION}/kafka-connect-protobuf-converter-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-protobuf-provider/${CONFLUENT_VERSION}/kafka-protobuf-provider-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-protobuf-types/${CONFLUENT_VERSION}/kafka-protobuf-types-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-protobuf-serializer/${CONFLUENT_VERSION}/kafka-protobuf-serializer-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-registry-client/${CONFLUENT_VERSION}/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-registry-client-encryption/${CONFLUENT_VERSION}/kafka-schema-registry-client-encryption-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-registry-client-encryption-tink/${CONFLUENT_VERSION}/kafka-schema-registry-client-encryption-tink-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/dek-registry-client/${CONFLUENT_VERSION}/dek-registry-client-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-serializer/${CONFLUENT_VERSION}/kafka-schema-serializer-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-converter/${CONFLUENT_VERSION}/kafka-schema-converter-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-schema-rules/${CONFLUENT_VERSION}/kafka-schema-rules-${CONFLUENT_VERSION}.jar" \
          "${MVN_BASE}/info/picocli/picocli/${PICOCLI_VERSION}/picocli-${PICOCLI_VERSION}.jar" \
          "${MVN_BASE}/com/google/crypto/tink/tink/${TINK_VERSION}/tink-${TINK_VERSION}.jar" \
          "${MVN_BASE}/com/google/code/findbugs/jsr305/${JSR305_VERSION}/jsr305-${JSR305_VERSION}.jar" \
          "${MVN_BASE}/com/google/code/gson/gson/${GSON_VERSION}/gson-${GSON_VERSION}.jar" \
          "${MVN_BASE}/com/google/errorprone/error_prone_annotations/${EPA_VERSION}/error_prone_annotations-${EPA_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-protobuf/${JACKSON_VERSION}/jackson-dataformat-protobuf-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/protoparser/${PROTOP_VERSION}/protoparser-${PROTOP_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-core/${NESSIE_VERSION}/cel-core-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-tools/${NESSIE_VERSION}/cel-tools-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-generated-pb/${NESSIE_VERSION}/cel-generated-pb-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-generated-antlr/${NESSIE_VERSION}/cel-generated-antlr-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/woodstox/woodstox-core/${WOODSTOX_VERSION}/woodstox-core-${WOODSTOX_VERSION}.jar" \
          "${MVN_BASE}/com/hubspot/jackson/jackson-datatype-protobuf/${JACKSON_PB_VERSION}/jackson-datatype-protobuf-${JACKSON_PB_VERSION}.jar" \
          "${MVN_BASE}/com/ibm/jsonata4java/JSONata4Java/${JSONATA_VERSION}/JSONata4Java-${JSONATA_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/wire/wire-schema-jvm/${SQUP_WIRE_VERSION}/wire-schema-jvm-${SQUP_WIRE_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/wire/wire-runtime-jvm/${SQUP_WIRE_VERSION}/wire-runtime-jvm-${SQUP_WIRE_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/okio/okio-jvm/${SQUP_OKIO_VERSION}/okio-jvm-${SQUP_OKIO_VERSION}.jar" \
          "${MVN_BASE}/com/palantir/javapoet/javapoet/${PLNTR_JAVAPOET_VERSION}/javapoet-${PLNTR_JAVAPOET_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/kotlinpoet-jvm/${SQUP_KOTLINPOET_VERSION}/kotlinpoet-jvm-${SQUP_KOTLINPOET_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/kotlin/kotlin-stdlib/${KOTLIN_VERSION}/kotlin-stdlib-${KOTLIN_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/kotlin/kotlin-reflect/${KOTLIN_VERSION}/kotlin-reflect-${KOTLIN_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/annotations/${JB_ANN_VERSION}/annotations-${JB_ANN_VERSION}.jar" \
          "${MVN_BASE}/com/google/protobuf/protobuf-java/${PROTOBUF_VERSION}/protobuf-java-${PROTOBUF_VERSION}.jar" \
          "${MVN_BASE}/com/google/protobuf/protobuf-java-util/${PROTOBUF_VERSION}/protobuf-java-util-${PROTOBUF_VERSION}.jar" \
          "${MVN_BASE}/com/google/api/grpc/proto-google-common-protos/${PROTO_COMMON_VERSION}/proto-google-common-protos-${PROTO_COMMON_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/failureaccess/${FA_VERSION}/failureaccess-${FA_VERSION}.jar" \
          "${MVN_BASE}/org/antlr/antlr4-runtime/${ANTLR4_VERSION}/antlr4-runtime-${ANTLR4_VERSION}.jar" \
          "${MVN_BASE}/org/apache/httpcomponents/client5/httpclient5/${HTTP_CLI_VERSION}/httpclient5-${HTTP_CLI_VERSION}.jar" \
          "${MVN_BASE}/org/apache/httpcomponents/core5/httpcore5/${HTTP_CORE_VERSION}/httpcore5-${HTTP_CORE_VERSION}.jar" \
          "${MVN_BASE}/org/apache/httpcomponents/core5/httpcore5-h2/${HTTP_CORE_VERSION}/httpcore5-h2-${HTTP_CORE_VERSION}.jar" \
       && for file in $(ls *.jar); do jar xvf $file; done \
       && rm -f *.jar \
       && jar cvf "confluent-protobuf-schema-client-${CONFLUENT_VERSION}.jar" [A-Z]* [a-z]* \
       && mv "confluent-protobuf-schema-client-${CONFLUENT_VERSION}.jar" / \
       && cd / && rm -rf WORKDIR


FROM   eclipse-temurin:25-jre
LABEL  maintainer="oracle@a2.solutions"
LABEL  vendor="A2 Rešitve d.o.o."
LABEL  version="2.15.0"
LABEL  release="2.15.0"
LABEL  name="oracdc: Oracle RDBMS CDC and data streaming"
LABEL  summary="oracdc and all dependencies for optimal work. When started, it will run the Kafka Connect framework in distributed mode."

ARG    CONFLUENT_VERSION
ARG    MVN_BASE

RUN    set -eux && apt-get update && apt-get --yes dist-upgrade && apt-get --yes install netcat-traditional tzdata bash wget adduser 
RUN    addgroup kafka && adduser --uid 1001 --ingroup kafka kafka
ENV    JAVA_HOME=/opt/java/openjdk 
ARG    BASEDIR=/opt

ARG    KAFKA_VERSION=4.2.0
ARG    SCALA_VERSION=2.13
ARG    KAFKA_FILENAME=kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
ENV    KAFKA_HOME=${BASEDIR}/kafka
ENV    PATH=${PATH}:${KAFKA_HOME}/bin:${KAFKA_HOME}/connect/bin
ENV    PROPS_FILE=${KAFKA_HOME}/config/oracdc-distributed.properties
ENV    KAFKA_OPTS="--enable-native-access=ALL-UNNAMED -Dmaverick.log.nothread=true -Dmaverick.log.console=true -Dmaverick.log.console.level=ERROR"
RUN    wget -q "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_FILENAME}" -O "/tmp/${KAFKA_FILENAME}" \
       && tar xvfz /tmp/${KAFKA_FILENAME} -C ${BASEDIR} \
       && rm /tmp/${KAFKA_FILENAME} \
       && ln -s ${BASEDIR}/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME} \
       && mkdir -p ${KAFKA_HOME}/connect/lib \
       && mkdir -p ${KAFKA_HOME}/connect/bin \
       && mkdir -p ${KAFKA_HOME}/connect/jmx \
       && chown -R kafka:kafka ${BASEDIR}/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME}
# BouncyCastle signed jars
ARG    BC_VERSION="jdk18on-1.83"
COPY   target/lib/bcprov-${BC_VERSION}.jar ${KAFKA_HOME}/libs
COPY   target/lib/bcpkix-${BC_VERSION}.jar ${KAFKA_HOME}/libs
COPY   target/lib/bcutil-${BC_VERSION}.jar ${KAFKA_HOME}/libs

ARG    ORACDC_VERSION=2.15.0
ARG    ORACDC_FILENAME=oracdc-kafka-${ORACDC_VERSION}-standalone.jar
COPY   target/${ORACDC_FILENAME} ${KAFKA_HOME}/connect/lib
COPY   config/connect-log4j.properties ${KAFKA_HOME}/config
COPY   config/connect-log4j2.yaml ${KAFKA_HOME}/config
RUN    chown -R kafka:kafka ${KAFKA_HOME}/connect/lib/${ORACDC_FILENAME}
RUN    chown -R kafka:kafka ${KAFKA_HOME}/config
ENV    ORACDC_JAR=${KAFKA_HOME}/connect/lib/${ORACDC_FILENAME}
ARG    MAIN_CONFIG=solutions.a2.cdc.oracle.utils.file.Env2Property
RUN    mkdir -p ${BASEDIR}/oracdc/licenses \
       && touch ${BASEDIR}/oracdc/run.sh \
       && chmod +x ${BASEDIR}/oracdc/run.sh \
       && echo "#!/bin/sh" > ${BASEDIR}/oracdc/run.sh \
       && echo "java -cp ${ORACDC_JAR} ${MAIN_CONFIG} A2_ORACDC_ ${PROPS_FILE} --append" >> ${BASEDIR}/oracdc/run.sh \
       && echo "cat ${PROPS_FILE}" >> ${BASEDIR}/oracdc/run.sh \
       && echo "connect-distributed.sh $PROPS_FILE" >> ${BASEDIR}/oracdc/run.sh \
       && chown -R kafka:kafka ${BASEDIR}/oracdc

USER   kafka
# CVE-2026-1605 BEGIN
ARG    JV="12.0.33"
RUN    cd ${KAFKA_HOME}/libs && rm -f jetty-*-12.0.22.jar && wget -q \
       "${MVN_BASE}/org/eclipse/jetty/jetty-http/${JV}/jetty-http-${JV}.jar" \
       "${MVN_BASE}/org/eclipse/jetty/jetty-security/${JV}/jetty-security-${JV}.jar" \
       "${MVN_BASE}/org/eclipse/jetty/jetty-session/${JV}/jetty-session-${JV}.jar" \
       "${MVN_BASE}/org/eclipse/jetty/jetty-client/${JV}/jetty-client-${JV}.jar" \
       "${MVN_BASE}/org/eclipse/jetty/jetty-io/${JV}/jetty-io-${JV}.jar" \
       "${MVN_BASE}/org/eclipse/jetty/jetty-server/${JV}/jetty-server-${JV}.jar" \
       "${MVN_BASE}/org/eclipse/jetty/jetty-util/${JV}/jetty-util-${JV}.jar" \
       "${MVN_BASE}/org/eclipse/jetty/ee10/jetty-ee10-servlet/${JV}/jetty-ee10-servlet-${JV}.jar" \
       "${MVN_BASE}/org/eclipse/jetty/ee10/jetty-ee10-servlets/${JV}/jetty-ee10-servlets-${JV}.jar" \
       "${MVN_BASE}/org/eclipse/jetty/jetty-alpn-client/${JV}/jetty-alpn-client-${JV}.jar"
# CVE-2026-1605 END
# GHSA-72hv-8253-57qq BEGIN
ARG    XV="2.21.1"
ARG    XVA="2.21"
ARG    SV="2.5"
RUN    cd ${KAFKA_HOME}/libs && rm -f jackson-*-2.19.2.jar && rm -f snakeyaml-2.4.jar && wget -q \
       "${MVN_BASE}/org/yaml/snakeyaml/${SV}/snakeyaml-${SV}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/core/jackson-annotations/${XVA}/jackson-annotations-${XVA}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/core/jackson-core/${XV}/jackson-core-${XV}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/core/jackson-databind/${XV}/jackson-databind-${XV}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-csv/${XV}/jackson-dataformat-csv-${XV}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/${XV}/jackson-dataformat-yaml-${XV}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/${XV}/jackson-datatype-jdk8-${XV}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/jakarta/rs/jackson-jakarta-rs-base/${XV}/jackson-jakarta-rs-base-${XV}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/jakarta/rs/jackson-jakarta-rs-json-provider/${XV}/jackson-jakarta-rs-json-provider-${XV}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/module/jackson-module-jakarta-xmlbind-annotations/${XV}/jackson-module-jakarta-xmlbind-annotations-${XV}.jar" \
       "${MVN_BASE}/com/fasterxml/jackson/module/jackson-module-blackbird/${XV}/jackson-module-blackbird-${XV}.jar"
# GHSA-72hv-8253-57qq END
# CVE-2025-67030 BEGIN
RUN    cd ${KAFKA_HOME}/libs && rm -f plexus-utils-*.jar
# CVE-2025-67030 END
# CVE-2026-34477/CVE-2026-34478/CVE-2026-34479/CVE-2026-34480 BEGIN
ARG    L4JV="2.25.4"
RUN    cd ${KAFKA_HOME}/libs && rm -f log4j-api-*.jar && rm -f log4j-core-*.jar && rm -f log4j-slf4j-impl-*.jar && wget -q \
       "${MVN_BASE}/org/apache/logging/log4j/log4j-api/${L4JV}/log4j-api-${L4JV}.jar" \
       "${MVN_BASE}/org/apache/logging/log4j/log4j-core/${L4JV}/log4j-core-${L4JV}.jar" \
       "${MVN_BASE}/org/apache/logging/log4j/log4j-slf4j-impl/${L4JV}/log4j-slf4j-impl-${L4JV}.jar"
# CVE-2026-34477/CVE-2026-34478/CVE-2026-34479/CVE-2026-34480 END
RUN    mkdir ${KAFKA_HOME}/logs
RUN    touch ${KAFKA_HOME}/logs/connect.log
COPY   LICENSE* ${BASEDIR}/oracdc
COPY   licenses/* ${BASEDIR}/oracdc/licenses

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

COPY   --from=build-sr-client /confluent-avro-schema-client-${CONFLUENT_VERSION}.jar ${KAFKA_HOME}/connect/lib
COPY   --from=build-sr-client /confluent-protobuf-schema-client-${CONFLUENT_VERSION}.jar ${KAFKA_HOME}/connect/lib

ARG    CHECKER_CMD=${KAFKA_HOME}/connect/bin/oraCheck.sh
RUN    echo "#! /bin/sh" > ${CHECKER_CMD} \
       && echo "java -cp \${ORACDC_JAR} solutions.a2.cdc.oracle.utils.OracleSetupCheck \"\$@\"" >> ${CHECKER_CMD} \
       && chmod +x ${CHECKER_CMD}

ARG    EXPORTER_VERSION=0.20.0
ARG    EXPORTER_FILE=jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar
RUN    cd ${KAFKA_HOME}/connect/jmx \
RUN    && wget -q "${MVN_BASE}/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/${EXPORTER_FILE}" \
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

