#!/bin/bash
# Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

DIRNAME=`dirname $0`
GLOBAL_CFG="$DIRNAME/scalaris.cfg.sh"
LOCAL_CFG="$DIRNAME/scalaris.local.cfg.sh"
if [ -f "$GLOBAL_CFG" ] ; then source "$GLOBAL_CFG" ; fi
if [ -f "$LOCAL_CFG" ] ; then source "$LOCAL_CFG" ; fi

export ERL_MAX_PORTS=16384
export NODES_VM=$1
A=$2

YAWS_PORT=$((8000+A))
CS_PORT=$((14195+A))
erl $ERL_OPTS -noinput -setcookie "chocolate chip cookie" -pa ../contrib/log4erl/ebin -pa ../contrib/yaws/ebin -pa ../ebin -yaws embedded true -connect_all false \
    -chordsharp cs_port $CS_PORT \
    -chordsharp yaws_port $YAWS_PORT \
    -name node$A -s bench_slave ;
