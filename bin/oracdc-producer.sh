#!/bin/sh
#
# Copyright (c) 2018-present, http://a2-solutions.eu
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

if type -p java; then
	JAVA=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
	JAVA="$JAVA_HOME/bin/java"
else
	echo "Unable to find java executable. Exiting!!!"
	exit 1
fi

THISCOMMAND="$0"
while [ -h "$THISCOMMAND" ]; do
	COMMANDDIR="$( cd -P "  $( dirname "$THISCOMMAND" )" >/dev/null 2>&1 && pwd )"
	THISCOMMAND="$(readlink "$THISCOMMAND")"
	[[ $THISCOMMAND != /* ]] && THISCOMMAND="$COMMANDDIR/$THISCOMMAND"
done
COMMANDDIR="$( cd -P "$( dirname "$THISCOMMAND" )" >/dev/null 2>&1 && pwd )"

if [ -z ${A2_CDC_HOME+x} ]; then
	A2_CDC_HOME="$(dirname "$COMMANDDIR")"
elif [ !  -d "$A2_CDC_HOME" ]; then
	echo "A2_CDC_HOME is set to non existent path '$A2_CDC_HOME'. Exiting!!!"
	exit 1
fi

nohup $JAVA \
	-cp $(for i in $A2_CDC_HOME/lib/*.jar ; do echo -n $i: ; done). \
	-Da2.log4j.configuration=$A2_CDC_HOME/conf/log4j.properties eu.solutions.a2.cdc.oracle.OraCdcProducer \
	$A2_CDC_HOME/conf/oracdc-producer.conf </dev/null 2>&1 | tee $A2_CDC_HOME/log/oracdc-producer.log &
