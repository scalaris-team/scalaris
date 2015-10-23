#!/bin/bash

source $(pwd)/env.sh

BASE_PORT=14195
BASE_YAWSPORT=8000
BASE_VM_IDX=0
KEYLIST=""
LOCAL_OFFSET=${SLURM_LOCALID}
GLOBAL_OFFSET=${SLURM_PROCID}
NODES_PER_VM=0

start_vm(){
    VM_IDX=$((BASE_VM_IDX+GLOBAL_OFFSET))
    JOIN_KEYS=`erl -name bench_${GLOBAL_OFFSET} -noinput -eval "L = lists:nth($VM_IDX, $KEYLIST), io:format('~p', [L]), halt(0)."`
    PORT=$((BASE_PORT+LOCAL_OFFSET))
    YAWSPORT=$((BASE_YAWSPORT+LOCAL_OFFSET))
    $BINDIR/scalarisctl -j "$JOIN_KEYS" -n node$PORT -p $PORT -y $YAWSPORT --nodes-per-vm $NODES_PER_VM --screen -d -t joining start
    sleep 5
}

until [ -z "$1" ]; do
    OPTIND=1
    case $1 in
        "--keylist")
            shift
            KEYLIST=$1
            shift;;
        "--vm-idx")
            shift
            BASE_VM_IDX=$1
            shift;;
        "--nodes-per-vm")
            shift
            NODES_PER_VM=$1
            shift;;
        *)
            echo $1
            shift
            usage 1
    esac
done

start_vm
