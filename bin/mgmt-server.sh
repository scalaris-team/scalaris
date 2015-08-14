#!/bin/bash
# Copyright 2007-2015 Zuse Institute Berlin
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# Script to start an empty Scalaris management server.
ID=0
NAME="mgmt_server"
PORT=$((14195+$ID))
YAWSPORT=$((8000+$ID))

ABSPATH="$(cd "${0%/*}" 2>/dev/null; echo "$PWD"/"${0##*/}")"
DIRNAME=`dirname $ABSPATH`

# start a mgmt_server
MGMT_NAME_AND_PORTS="-n mgmt_server -p 14194 -y 7999 "
$DIRNAME/scalarisctl $MGMT_NAME_AND_PORTS -t nostart -m "$@" start
