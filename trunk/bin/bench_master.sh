#!/bin/bash
# Copyright 2007-2010 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

ABSPATH="$(cd "${0%/*}" 2>/dev/null; echo "$PWD"/"${0##*/}")"
DIRNAME=`dirname $ABSPATH`

if [ 4 != $# ] ; then
    echo Usage: $DIRNAME/bench_master nodes_per_vm workers iterations ring_size
    exit 1 ;
fi

export ERL_MAX_PORTS=16384
export NODES_VM=$1
export WORKER=$2
export ITERATIONS=$3
export RING_SIZE=$4

$DIRNAME/scalarisctl bench_master
