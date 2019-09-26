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
if [[ `id -u` -ne 0 ]] ; then
	 echo "Please run as root" ; exit 1 ;
fi

ORACDC_HOME=/opt/a2/agents/oracdc

mkdir -p $ORACDC_HOME/lib
cp target/lib/*.jar $ORACDC_HOME/lib

cp target/oracdc-kafka-0.9.0.jar $ORACDC
cp oracdc-producer.sh $ORACDC
cp oracdc-producer.conf $ORACDC
cp log4j.properties $ORACDC

chmod +x $ORACDC_HOME/oracdc-producer.sh

