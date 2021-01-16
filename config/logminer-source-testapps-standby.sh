#!/bin/sh
CONNECT_HOME=/kafka/connect
LIB_HOME=$CONNECT_HOME/lib
# Add Chronicle Queue jar's
CLASSPATH=$LIB_HOME/affinity-3.2.2.jar:$LIB_HOME/annotations-12.0.jar:$LIB_HOME/chronicle-bytes-2.17.49.jar:$LIB_HOME/chronicle-core-2.17.35.jar:$LIB_HOME/chronicle-queue-5.17.43.jar:$LIB_HOME/chronicle-threads-2.17.27.jar:$LIB_HOME/chronicle-wire-2.17.71.jar:$LIB_HOME/compiler-2.3.4.jar:$LIB_HOME/hamcrest-core-1.3.jar:$LIB_HOME/jna-4.2.1.jar:$LIB_HOME/jna-platform-4.2.1.jar
# Add Oracle jar's
CLASSPATH=$LIB_HOME/ojdbc8.jar:$LIB_HOME/ucp.jar:$LIB_HOME/oraclepki.jar:$LIB_HOME/osdt_core.jar:$LIB_HOME/osdt_cert.jar:${CLASSPATH}
# Add misc jar's
CLASSPATH=$LIB_HOME/commons-math3-3.6.1.jar:${CLASSPATH}
export CLASSPATH
connect-standalone -daemon /kafka/config/connect-avro-standalone.properties $CONNECT_HOME/config/logminer-source-testapps-standby.properties
