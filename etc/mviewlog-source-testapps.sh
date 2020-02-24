#!/bin/sh
CONNECT_HOME=/kafka/connect
LIB_HOME=$CONNECT_HOME/lib
# Add Oracle jar's
CLASSPATH=$LIB_HOME/ojdbc8.jar:$LIB_HOME/ucp.jar:$LIB_HOME/oraclepki.jar:$LIB_HOME/osdt_core.jar:$LIB_HOME/osdt_cert.jar:${CLASSPATH}
export CLASSPATH
connect-standalone -daemon /kafka/config/connect-avro-standalone.properties $CONNECT_HOME/config/mviewlog-source-testapps.properties
