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

RINGSIZE=4
SCALARIS_UNITTEST_PORT=${SCALARIS_UNITTEST_PORT-"14195"}
SCALARIS_YAWS_PORT=${SCALARIS_YAWS_PORT-"8000"}
usage(){
    echo "usage: setup-ring-for-benchmarks [options] <cmd>"
    echo " options:"
    echo "    --ring-size <number>   - the number of nodes"
    echo " <cmd>:"
    echo "    start       - start a ring"
    echo "    stop        - stop a ring"
    exit $1
}

start_ring(){
    KEYS=`./bin/scalarisctl -t nostart -e "-noinput -eval \"io:format('\n%~p', [api_rt:escaped_list_of_keys(api_rt:get_evenly_spaced_keys($RINGSIZE))]), halt(0)\"" start | grep % | cut -c 3- | rev | cut -c 2- | rev`

    SCALARIS_ADDITIONAL_PARAMETERS="-scalaris mgmt_server {{127,0,0,1},$SCALARIS_UNITTEST_PORT,mgmt_server} -scalaris known_hosts [{{127,0,0,1},$SCALARIS_UNITTEST_PORT,service_per_vm}]"
    idx=0
    for key in $KEYS; do
        let idx+=1

        STARTTYPE=null
        if [ "x$idx" == x1 ]
        then
            STARTTYPE="first -m"
            TESTPORT=$SCALARIS_UNITTEST_PORT
            YAWSPORT=$SCALARIS_YAWS_PORT
        else
            STARTTYPE="joining"
            let TESTPORT=$SCALARIS_UNITTEST_PORT+$idx
            let YAWSPORT=$SCALARIS_YAWS_PORT+$idx
        fi

        ./bin/scalarisctl -d -k $key -n "ebench_node$idx" -p $TESTPORT -y $YAWSPORT -t $STARTTYPE start

    done
    ./bin/scalarisctl -n "ebench_node1" dbg-check-ring $RINGSIZE 30
}

stop_ring(){
    for idx in `seq 1 $RINGSIZE`; do
        ./bin/scalarisctl -n "ebench_node$idx" stop
    done
}

until [ -z "$1" ]; do
    OPTIND=1
    case $1 in
        "--help")
            shift
            usage 0;;
        "--ring-size")
            shift
            RINGSIZE=$1
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
