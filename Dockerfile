#
# This file is part of the oracdc project.
# Copyright (c) 2018-present, A2 Rešitve d.o.o.
# Authors: Aleksei Veremeev
#
# This program is offered under a commercial and under the AGPL license.
# For commercial licensing, contact us at sales@a2.solutions.
# For AGPL licensing, see below.
#
# AGPL licensing:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License along with this program; see the file GNU-AGPL-v3.0.adoc.
# If not, see <https://www.gnu.org/licenses/>.
#

ARG    MVN_BASE="https://repo1.maven.org/maven2"
ARG    LZ4_VERSION=1.11.4

#
# Kafka Connect plugins, one uber jar each: schema registry converters (Apicurio, Confluent;
# Avro, Protobuf) and the Strimzi Kubernetes config providers.
# Versions and contents: connect-plugins/pom.xml
#
FROM   maven:3.9.11-eclipse-temurin-25 AS build-connect-plugins
COPY   connect-plugins /build
COPY   licenses/Confluent-Community-License /licenses/
RUN    cd /build \
       && mvn -B -q -C clean package \
       && mkdir /out \
       && find . -path './*/*/target/*.jar' ! -name 'original-*' -exec cp {} /out \;

FROM   eclipse-temurin:25-jre
LABEL  maintainer="oracle@a2.solutions"
LABEL  vendor="A2 Rešitve d.o.o."
LABEL  version="2.15.7"
LABEL  release="2.15.7"
LABEL  name="oracdc: Oracle RDBMS CDC and data streaming"
LABEL  summary="oracdc and all dependencies for optimal work. When started, it will run the Kafka Connect framework in distributed mode."

ARG    MVN_BASE
ARG    LZ4_VERSION

RUN    set -eux && apt-get update && apt-get --yes dist-upgrade && apt-get --yes install netcat-traditional tzdata bash wget adduser 
RUN    addgroup kafka && adduser --uid 1001 --ingroup kafka kafka
ENV    JAVA_HOME=/opt/java/openjdk 
ARG    BASEDIR=/opt

ARG    KAFKA_VERSION=4.4.0
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
ARG    BC_VERSION="jdk18on-1.86"
COPY   target/lib/bcprov-${BC_VERSION}.jar ${KAFKA_HOME}/libs
COPY   target/lib/bcpkix-${BC_VERSION}.jar ${KAFKA_HOME}/libs
COPY   target/lib/bcutil-${BC_VERSION}.jar ${KAFKA_HOME}/libs
ARG    ORACDC_VERSION=2.15.7
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
RUN    mkdir ${KAFKA_HOME}/logs
RUN    touch ${KAFKA_HOME}/logs/connect.log
COPY   LICENSE* ${BASEDIR}/oracdc
COPY   licenses/* ${BASEDIR}/oracdc/licenses
# GHSA-47qp-hqvx-6r3f/GHSA-2r2c-cx56-8933 BEGIN
RUN    rm -f ${KAFKA_HOME}/libs/jline-*.jar
# GHSA-47qp-hqvx-6r3f/GHSA-2r2c-cx56-8933 END
# CVE-2026-93451/CVE-2026-108106 BEGIN
ARG    SNAPPY=1.1.10.10
RUN    cd ${KAFKA_HOME}/libs \
       && rm -f snappy-java*.jar \
       && wget -q "${MVN_BASE}/org/xerial/snappy/snappy-java/${SNAPPY}/snappy-java-${SNAPPY}.jar"
# CVE-2026-93451/CVE-2026-108106 END
# CVE-2026-106451/CVE-2026-59949/CVE-2026-106452/CVE-2026-106450/CVE-2026-106453 BEGIN
RUN    cd ${KAFKA_HOME}/libs \
       && rm -f lz4-java*.jar \
       && wget -q "${MVN_BASE}/at/yawk/lz4/lz4-java/${LZ4_VERSION}/lz4-java-${LZ4_VERSION}.jar"
# CVE-2026-106451/CVE-2026-59949/CVE-2026-106452/CVE-2026-106450/CVE-2026-106453 END
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

COPY   --from=build-connect-plugins /out/ ${KAFKA_HOME}/connect/lib/

ARG    CHECKER_CMD=${KAFKA_HOME}/connect/bin/oraCheck.sh
RUN    echo "#! /bin/sh" > ${CHECKER_CMD} \
       && echo "java -cp \${ORACDC_JAR} solutions.a2.cdc.oracle.utils.OracleSetupCheck \"\$@\"" >> ${CHECKER_CMD} \
       && chmod +x ${CHECKER_CMD}

ARG    EXPORTER_VERSION=0.20.0
ARG    EXPORTER_FILE=jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar
RUN    cd ${KAFKA_HOME}/connect/jmx \
       && wget -q "${MVN_BASE}/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/${EXPORTER_FILE}" \
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

