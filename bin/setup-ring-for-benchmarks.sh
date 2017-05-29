#!/bin/bash
# Copyright 2007-2017 Zuse Institute Berlin
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

RINGSIZE=4
NODEPREFIX=ebench_node
BADNODES=0
WITHSSL=""
SCALARIS_UNITTEST_PORT=${SCALARIS_UNITTEST_PORT-"14195"}
SCALARIS_UNITTEST_YAWS_PORT=${SCALARIS_UNITTEST_YAWS_PORT-"8000"}
usage(){
    echo "usage: setup-ring-for-benchmarks [options] <cmd>"
    echo " options:"
    echo "    --ring-size <number>   - the number of nodes"
    echo "    --ssl                  - enable ssl"
    echo "    --ssl-strict           - enable ssl with CA"
    echo "    --bad-nodes <number>   - the number of nodes with different CA"
    echo " <cmd>:"
    echo "    start       - start a ring"
    echo "    stop        - stop a ring"
    exit $1
}

start_ring(){
    KEYS=`./bin/scalarisctl -t nostart -n "${NODEPREFIX}" -e "-noinput -eval \"io:format('\n%~p', [api_rt:escaped_list_of_keys(api_rt:get_evenly_spaced_keys($RINGSIZE))]), halt(0)\"" start | grep % | cut -c 3- | rev | cut -c 2- | rev`

    export SCALARIS_ADDITIONAL_PARAMETERS="-scalaris mgmt_server {{127,0,0,1},$SCALARIS_UNITTEST_PORT,mgmt_server} -scalaris known_hosts [{{127,0,0,1},$SCALARIS_UNITTEST_PORT,service_per_vm}]"
    idx=0
    for key in $KEYS; do
        let idx+=1

        STARTTYPE=null
        if [ "x$idx" == x1 ]
        then
            STARTTYPE="first -m"
            TESTPORT=$SCALARIS_UNITTEST_PORT
            YAWSPORT=$SCALARIS_UNITTEST_YAWS_PORT
        else
            STARTTYPE="joining"
            let TESTPORT=$SCALARIS_UNITTEST_PORT+$idx-1
            let YAWSPORT=$SCALARIS_UNITTEST_YAWS_PORT+$idx-1
        fi

        ./bin/scalarisctl -d -k $key -n "${NODEPREFIX}$idx" -p $TESTPORT -y $YAWSPORT -t $STARTTYPE start $WITHSSL

    done

    for nodes in `seq 1 $BADNODES`; do
        let idx+=1
        let TESTPORT=$SCALARIS_UNITTEST_PORT+$idx-1
        let YAWSPORT=$SCALARIS_UNITTEST_YAWS_PORT+$idx-1
        ./bin/scalarisctl -d -n "${NODEPREFIX}$idx" -p $TESTPORT -y $YAWSPORT -t joining -s scalaris.local.ssl.bad.cfg start
    done

    if [ $BADNODES -ne 0 ]; then
        sleep 5 ## give bad nodes a chance to join
    fi

    ./bin/scalarisctl -n "${NODEPREFIX}1" dbg-check-ring $RINGSIZE 30
}

stop_ring(){
    let NODES=$RINGSIZE+$BADNODES
    for idx in `seq 1 $NODES`; do
        ./bin/scalarisctl -n "${NODEPREFIX}$idx" stop
    done
}

until [ -z "$1" ]; do
    OPTIND=1
    case $1 in
        "--help")
            shift
            usage 0;;
        "--ssl")
            WITHSSL="-s scalaris.local.ssl.cfg"
            shift;;
        "--ssl-strict")
            WITHSSL="-s scalaris.local.ssl.good.cfg"
            shift;;
        "--bad-nodes")
            shift
            BADNODES=$1
            shift;;
        "--ring-size")
            shift
            RINGSIZE=$1
            shift;;
        "--node-prefix")
            shift
            NODEPREFIX=$1
            shift;;
        start | stop)
            cmd="$1"
            shift;;
        *)
            shift
            usage 1
    esac
done

case $cmd in
    start)
        start_ring;;
    stop)
        stop_ring;;
    *)
        echo "Unknown command: $cmd."
        usage 1;;
esac
