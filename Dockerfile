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

ARG    CONFLUENT_VERSION=7.7.1
ARG    MVN_BASE="https://repo1.maven.org/maven2"

FROM   eclipse-temurin:21-jdk AS build-sr-client
RUN    set -eux && apt-get --yes install wget 

# Add schema registry dependencies
ARG    CONFLUENT_VERSION
ARG    MVN_BASE
ARG    CONFLUENT_BASE="https://packages.confluent.io/maven/io/confluent"
ARG    GUAVA_VERSION=33.3.0-jre
ARG    FA_VERSION=1.0.2
ARG    KOTLIN_VERSION=1.9.25
ARG    PICOCLI_VERSION=4.7.6
ARG    TINK_VERSION=1.15.0
ARG    JSR305_VERSION=3.0.2
ARG    GSON_VERSION=2.11.0
ARG    EPA_VERSION=2.32.0
ARG    ANTLR4_VERSION=4.13.2
ARG    JACKSON_VERSION=2.17.2
ARG    PROTOP_VERSION=4.0.3
ARG    SYAML_VERSION=2.3
ARG    NESSIE_VERSION=0.3.21
ARG    WOODSTOX_VERSION=6.7.0
ARG    JACKSON_PB_VERSION=0.9.15
ARG    JSONATA_VERSION=2.4.9

#
# Confluent AVRO support uber-jar
#
ARG    AVRO_VERSION=1.12.0
ARG    C_COMPRESS_VERSION=1.27.1
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
          "${MVN_BASE}/org/antlr/antlr4-runtime/${ANTLR4_VERSION}/antlr4-runtime-${ANTLR4_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/core/jackson-core/${JACKSON_VERSION}/jackson-core-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/core/jackson-databind/${JACKSON_VERSION}/jackson-databind-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/core/jackson-annotations/${JACKSON_VERSION}/jackson-annotations-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/${JACKSON_VERSION}/jackson-dataformat-yaml-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-protobuf/${JACKSON_VERSION}/jackson-dataformat-protobuf-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/protoparser/${PROTOP_VERSION}/protoparser-${PROTOP_VERSION}.jar" \
          "${MVN_BASE}/org/yaml/snakeyaml/${SYAML_VERSION}/snakeyaml-${SYAML_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-core/${NESSIE_VERSION}/cel-core-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-tools/${NESSIE_VERSION}/cel-tools-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-generated-pb/${NESSIE_VERSION}/cel-generated-pb-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-generated-antlr/${NESSIE_VERSION}/cel-generated-antlr-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/woodstox/woodstox-core/${WOODSTOX_VERSION}/woodstox-core-${WOODSTOX_VERSION}.jar" \
          "${MVN_BASE}/com/hubspot/jackson/jackson-datatype-protobuf/${JACKSON_PB_VERSION}/jackson-datatype-protobuf-${JACKSON_PB_VERSION}.jar" \
          "${MVN_BASE}/com/ibm/jsonata4java/JSONata4Java/${JSONATA_VERSION}/JSONata4Java-${JSONATA_VERSION}.jar" \
          "${MVN_BASE}/org/apache/avro/avro/${AVRO_VERSION}/avro-${AVRO_VERSION}.jar" \
          "${MVN_BASE}/org/apache/commons/commons-compress/${C_COMPRESS_VERSION}/commons-compress-${C_COMPRESS_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/failureaccess/${FA_VERSION}/failureaccess-${FA_VERSION}.jar" \
       && for file in $(ls *.jar); do jar xvf $file; done \
       && rm -f *.jar \
       && jar cvf "confluent-avro-schema-client-${CONFLUENT_VERSION}.jar" [A-Z]* [a-z]* \
       && mv "confluent-avro-schema-client-${CONFLUENT_VERSION}.jar" / \
       && cd / && rm -rf WORKDIR

#
# Confluent PROTOBUF support uber-jar
#
ARG    SQUP_WIRE_VERSION=4.9.9
ARG    SQUP_OKIO_VERSION=3.9.1
ARG    SQUP_JAVAPOET_VERSION=1.13.0
ARG    SQUP_KOTLINPOET_VERSION=1.18.1
ARG    PROTOBUF_VERSION=3.25.4
ARG    PROTO_COMMON_VERSION=2.44.0
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
          "${MVN_BASE}/org/antlr/antlr4-runtime/${ANTLR4_VERSION}/antlr4-runtime-${ANTLR4_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/core/jackson-core/${JACKSON_VERSION}/jackson-core-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/core/jackson-databind/${JACKSON_VERSION}/jackson-databind-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/core/jackson-annotations/${JACKSON_VERSION}/jackson-annotations-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/${JACKSON_VERSION}/jackson-dataformat-yaml-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-protobuf/${JACKSON_VERSION}/jackson-dataformat-protobuf-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/protoparser/${PROTOP_VERSION}/protoparser-${PROTOP_VERSION}.jar" \
          "${MVN_BASE}/org/yaml/snakeyaml/${SYAML_VERSION}/snakeyaml-${SYAML_VERSION}.jar" \
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
          "${MVN_BASE}/com/squareup/javapoet/${SQUP_JAVAPOET_VERSION}/javapoet-${SQUP_JAVAPOET_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/kotlinpoet-jvm/${SQUP_KOTLINPOET_VERSION}/kotlinpoet-jvm-${SQUP_KOTLINPOET_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/kotlin/kotlin-stdlib/${KOTLIN_VERSION}/kotlin-stdlib-${KOTLIN_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/kotlin/kotlin-reflect/${KOTLIN_VERSION}/kotlin-reflect-${KOTLIN_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/annotations/${JB_ANN_VERSION}/annotations-${JB_ANN_VERSION}.jar" \
          "${MVN_BASE}/com/google/protobuf/protobuf-java/${PROTOBUF_VERSION}/protobuf-java-${PROTOBUF_VERSION}.jar" \
          "${MVN_BASE}/com/google/protobuf/protobuf-java-util/${PROTOBUF_VERSION}/protobuf-java-util-${PROTOBUF_VERSION}.jar" \
          "${MVN_BASE}/com/google/api/grpc/proto-google-common-protos/${PROTO_COMMON_VERSION}/proto-google-common-protos-${PROTO_COMMON_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/failureaccess/${FA_VERSION}/failureaccess-${FA_VERSION}.jar" \
       && for file in $(ls *.jar); do jar xvf $file; done \
       && rm -f *.jar \
       && jar cvf "confluent-protobuf-schema-client-${CONFLUENT_VERSION}.jar" [A-Z]* [a-z]* \
       && mv "confluent-protobuf-schema-client-${CONFLUENT_VERSION}.jar" / \
       && cd / && rm -rf WORKDIR

#
# Confluent JSON Schema support uber-jar
#
ARG    JSON_VERSION=20240303
ARG    VLDTR_VERSION=1.9.0
ARG    RE2J_VERSION=1.7
ARG    JODA_VERSION=2.12.7
ARG    URI_TMPL_VERSION=2.1.8
ARG    EVERIT_VERSION=1.14.4
ARG    SKEMA_VERSION=0.16.0
ARG    MBKNOR_VERSION=1.0.39
ARG    SCALA_VERSION=2.13.14
ARG    VLDTN_API_VERSION=2.0.1.Final
ARG    CLASSGRAPH_VERSION=4.8.175
RUN    WORKDIR=/tmp/$RANDOM && mkdir -p $WORKDIR && cd $WORKDIR \
       && wget -q \
          "${CONFLUENT_BASE}/kafka-connect-json-schema-converter/${CONFLUENT_VERSION}/kafka-connect-json-schema-converter-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-json-schema-provider/${CONFLUENT_VERSION}/kafka-json-schema-provider-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-json-schema-serializer/${CONFLUENT_VERSION}/kafka-json-schema-serializer-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/kafka-json-serializer/${CONFLUENT_VERSION}/kafka-json-serializer-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/common-config/${CONFLUENT_VERSION}/common-config-${CONFLUENT_VERSION}.jar" \
          "${CONFLUENT_BASE}/common-utils/${CONFLUENT_VERSION}/common-utils-${CONFLUENT_VERSION}.jar" \
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
          "${MVN_BASE}/org/antlr/antlr4-runtime/${ANTLR4_VERSION}/antlr4-runtime-${ANTLR4_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/core/jackson-core/${JACKSON_VERSION}/jackson-core-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/core/jackson-databind/${JACKSON_VERSION}/jackson-databind-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/core/jackson-annotations/${JACKSON_VERSION}/jackson-annotations-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/${JACKSON_VERSION}/jackson-dataformat-yaml-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/dataformat/jackson-dataformat-protobuf/${JACKSON_VERSION}/jackson-dataformat-protobuf-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/squareup/protoparser/${PROTOP_VERSION}/protoparser-${PROTOP_VERSION}.jar" \
          "${MVN_BASE}/org/yaml/snakeyaml/${SYAML_VERSION}/snakeyaml-${SYAML_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-core/${NESSIE_VERSION}/cel-core-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-tools/${NESSIE_VERSION}/cel-tools-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-generated-pb/${NESSIE_VERSION}/cel-generated-pb-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/org/projectnessie/cel/cel-generated-antlr/${NESSIE_VERSION}/cel-generated-antlr-${NESSIE_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/woodstox/woodstox-core/${WOODSTOX_VERSION}/woodstox-core-${WOODSTOX_VERSION}.jar" \
          "${MVN_BASE}/com/hubspot/jackson/jackson-datatype-protobuf/${JACKSON_PB_VERSION}/jackson-datatype-protobuf-${JACKSON_PB_VERSION}.jar" \
          "${MVN_BASE}/com/ibm/jsonata4java/JSONata4Java/${JSONATA_VERSION}/JSONata4Java-${JSONATA_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/${JACKSON_VERSION}/jackson-datatype-jdk8-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/datatype/jackson-datatype-jsr310/${JACKSON_VERSION}/jackson-datatype-jsr310-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/datatype/jackson-datatype-joda/${JACKSON_VERSION}/jackson-datatype-joda-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/datatype/jackson-datatype-guava/${JACKSON_VERSION}/jackson-datatype-guava-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/com/fasterxml/jackson/module/jackson-module-parameter-names/${JACKSON_VERSION}/jackson-module-parameter-names-${JACKSON_VERSION}.jar" \
          "${MVN_BASE}/org/json/json/${JSON_VERSION}/json-${JSON_VERSION}.jar" \
          "${MVN_BASE}/joda-time/joda-time/${JODA_VERSION}/joda-time-${JODA_VERSION}.jar" \
          "${MVN_BASE}/commons-validator/commons-validator/${VLDTR_VERSION}/commons-validator-${VLDTR_VERSION}.jar" \
          "${MVN_BASE}/com/google/re2j/re2j/${RE2J_VERSION}/re2j-${RE2J_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/kotlin/kotlin-stdlib/${KOTLIN_VERSION}/kotlin-stdlib-${KOTLIN_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/kotlin/kotlin-script-runtime/${KOTLIN_VERSION}/kotlin-script-runtime-${KOTLIN_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/kotlin/kotlin-scripting-common/${KOTLIN_VERSION}/kotlin-scripting-common-${KOTLIN_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/kotlin/kotlin-scripting-jvm/${KOTLIN_VERSION}/kotlin-scripting-jvm-${KOTLIN_VERSION}.jar" \
          "${MVN_BASE}/org/jetbrains/kotlin/kotlin-scripting-compiler-embeddable/${KOTLIN_VERSION}/kotlin-scripting-compiler-embeddable-${KOTLIN_VERSION}.jar" \
          "${MVN_BASE}/javax/validation/validation-api/${VLDTN_API_VERSION}/validation-api-${VLDTN_API_VERSION}.jar" \
          "${MVN_BASE}/org/scala-lang/scala-library/${SCALA_VERSION}/scala-library-${SCALA_VERSION}.jar" \
          "${MVN_BASE}/com/damnhandy/handy-uri-templates/${URI_TMPL_VERSION}/handy-uri-templates-${URI_TMPL_VERSION}.jar" \
          "${MVN_BASE}/io/github/classgraph/classgraph/${CLASSGRAPH_VERSION}/classgraph-${CLASSGRAPH_VERSION}.jar" \
          "${MVN_BASE}/com/github/erosb/everit-json-schema/${EVERIT_VERSION}/everit-json-schema-${EVERIT_VERSION}.jar" \
          "${MVN_BASE}/com/github/erosb/json-sKema/${SKEMA_VERSION}/json-sKema-${SKEMA_VERSION}.jar" \
          "${MVN_BASE}/com/kjetland/mbknor-jackson-jsonschema_2.13/${MBKNOR_VERSION}/mbknor-jackson-jsonschema_2.13-${MBKNOR_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar" \
          "${MVN_BASE}/com/google/guava/failureaccess/${FA_VERSION}/failureaccess-${FA_VERSION}.jar" \
       && for file in $(ls *.jar); do jar xvf $file; done \
       && rm -f *.jar \
       && jar cvf "confluent-json-schema-client-${CONFLUENT_VERSION}.jar" [A-Z]* [a-z]* \
       && mv "confluent-json-schema-client-${CONFLUENT_VERSION}.jar" / \
       && cd / && rm -rf WORKDIR


FROM   eclipse-temurin:21-jre
LABEL  maintainer="oracle@a2-solutions.eu"
LABEL  vendor="A2 Rešitve d.o.o."
LABEL  version="2.6.0"
LABEL  release="2.6.0"
LABEL  name="oracdc: Oracle RDBMS CDC and data streaming"
LABEL  summary="oracdc and all dependencies for optimal work. When started, it will run the Kafka Connect framework in distributed mode."

ARG    CONFLUENT_VERSION
ARG    MVN_BASE

RUN    set -eux && apt-get update && apt-get --yes install netcat-traditional tzdata bash wget adduser 
RUN    addgroup kafka && adduser --uid 1001 --ingroup kafka kafka 
ARG    BASEDIR=/opt

ARG    KAFKA_VERSION=3.9.0
ARG    SCALA_VERSION=2.13
ARG    KAFKA_FILENAME=kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
ENV    KAFKA_HOME=${BASEDIR}/kafka
ENV    PATH=${PATH}:${KAFKA_HOME}/bin:${KAFKA_HOME}/connect/bin
ENV    PROPS_FILE=${KAFKA_HOME}/config/oracdc-distributed.properties
ENV    KAFKA_OPTS="-Dchronicle.analytics.disable=true -Dchronicle.disk.monitor.disable=true -Dchronicle.queue.warnSlowAppenderMs=500 --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports jdk.unsupported/sun.misc=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
RUN    wget -q "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_FILENAME}" -O "/tmp/${KAFKA_FILENAME}" \
       && tar xvfz /tmp/${KAFKA_FILENAME} -C ${BASEDIR} \
       && rm /tmp/${KAFKA_FILENAME} \
       && ln -s ${BASEDIR}/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME} \
       && mkdir -p ${KAFKA_HOME}/connect/lib \
       && mkdir -p ${KAFKA_HOME}/connect/bin \
       && mkdir -p ${KAFKA_HOME}/connect/jmx \
       && chown -R kafka:kafka ${BASEDIR}/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME}

ARG    ORACDC_VERSION=2.6.0
ARG    ORACDC_FILENAME=oracdc-kafka-${ORACDC_VERSION}-standalone.jar
COPY   target/${ORACDC_FILENAME} ${KAFKA_HOME}/connect/lib
COPY   config/connect-log4j.properties ${KAFKA_HOME}/config
RUN    chown -R kafka:kafka ${KAFKA_HOME}/connect/lib/${ORACDC_FILENAME}
RUN    chown -R kafka:kafka ${KAFKA_HOME}/config
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

COPY   --from=build-sr-client /confluent-avro-schema-client-${CONFLUENT_VERSION}.jar ${KAFKA_HOME}/connect/lib
COPY   --from=build-sr-client /confluent-protobuf-schema-client-${CONFLUENT_VERSION}.jar ${KAFKA_HOME}/connect/lib
COPY   --from=build-sr-client /confluent-json-schema-client-${CONFLUENT_VERSION}.jar ${KAFKA_HOME}/connect/lib

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

