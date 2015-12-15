#!/bin/bash

# DHT_NODES_PER_VM, VMS_PER_NODE, KEYLIST inherited from environment
BASE_PORT=14195
BASE_YAWSPORT=8000
BASE_VM_IDX=$VMS_PER_NODE
LOCAL_OFFSET=${SLURM_LOCALID} # Node local task ID for the process within a job
GLOBAL_OFFSET=${SLURM_PROCID} # relative process ID of the current process, 0 based

start_vm(){
    # uncomment (incl line 24) to restrict the number of schedulers for one node only
    # local old_erl_sched_flags=$ERL_SCHED_FLAGS
    # [[ $GLOBAL_OFFSET = 0 ]] && ERL_SCHED_FLAGS="+S 1"

    # Delay joining depending on the local offset. Necessary for starting very large rings.
    local sleeptime=$((LOCAL_OFFSET*2))
    sleep $sleeptime

    VM_IDX=$((BASE_VM_IDX+GLOBAL_OFFSET+1))
    JOIN_KEYS=`erl -name bench_${GLOBAL_OFFSET} -noinput -eval "L = lists:nth($VM_IDX, $KEYLIST), io:format('~p', [L]), halt(0)."`
    PORT=$((BASE_PORT+LOCAL_OFFSET))
    YAWSPORT=$((BASE_YAWSPORT+LOCAL_OFFSET))
    $BINDIR/scalarisctl -j "$JOIN_KEYS" -n node$PORT -p $PORT -y $YAWSPORT --nodes-per-vm $DHT_NODES_PER_VM --screen -d -t joining ${SCALARISCTL_PARAMS:+$SCALARISCTL_PARAMS} start
    # ERL_SCHED_FLAGS=$old_erl_sched_flags
}

start_vm
