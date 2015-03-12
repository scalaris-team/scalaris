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

# Script to start a new Scalaris system with an initial node.
ID=0
NAME="firstnode"
PORT=$((14195+$ID))
YAWSPORT=$((8000+$ID))

ABSPATH="$(cd "${0%/*}" 2>/dev/null; echo "$PWD"/"${0##*/}")"
DIRNAME=`dirname $ABSPATH`

if [ 1 -le $# ]; then
ERLFLAGS="$@"
else
ERLFLAGS=" "
fi

# start a mgmt_server (-m)
# start the first node (-t first)
NODE_NAME_AND_PORTS="-n $NAME -p $PORT -y $YAWSPORT"
$DIRNAME/scalarisctl -e "$ERLFLAGS" -m $NODE_NAME_AND_PORTS -t first start
