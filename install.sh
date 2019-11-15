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
###if [[ `id -u` -ne 0 ]] ; then
###	 echo "Please run as root" ; exit 1 ;
###fi

if [ -z "$1" ]; then
	echo "Usage: install.sh <PATH2INSTALL>"
	exit 1
fi
A2_CDC_HOME="$1"

mkdir -p $A2_CDC_HOME/bin
mkdir -p $A2_CDC_HOME/conf
mkdir -p $A2_CDC_HOME/doc
mkdir -p $A2_CDC_HOME/lib
mkdir -p $A2_CDC_HOME/log

cp target/lib/*.jar $A2_CDC_HOME/lib
cp target/oracdc-*.jar $A2_CDC_HOME/lib
cp bin/* $A2_CDC_HOME/bin
cp conf/* $A2_CDC_HOME/conf
cp doc/* $A2_CDC_HOME/doc

chmod +x $A2_CDC_HOME/bin/*.sh


