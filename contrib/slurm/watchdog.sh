#!/bin/bash

source $(pwd)/env.sh

function cleanup_node(){
    screen -ls | grep Detached | grep scalaris_ | grep "SLURM_JOBID_${SLURM_JOB_ID}" | cut -d. -f1 | awk '{print $1}' | xargs -r kill

    return 0
}

IS_RUNNING=($(sacct -j $SLURM_JOB_ID -b | grep $SLURM_JOB_ID | awk '{print $2}'))
while [[ ${IS_RUNNING[0]} == "RUNNING" ]]; do
    sleep $WATCHDOG_INTERVAL
    IS_RUNNING=($(sacct -j $SLURM_JOB_ID -b | grep $SLURM_JOB_ID | awk '{print $2}'))
done

cleanup_node

