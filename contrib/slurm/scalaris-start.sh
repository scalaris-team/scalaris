#!/bin/bash

source $(pwd)/env.sh

function fix_known_hosts() {
    if [ -e $ETCDIR/scalaris.local.cfg ]
    then
        mv $ETCDIR/scalaris.local.cfg .
    else
        touch $ETCDIR/scalaris.local.cfg
    fi
    NODEIDX=1
    echo "{known_hosts, [" >> $ETCDIR/scalaris.local.cfg
    for NODE in `scontrol show hostnames`; do
        PORT=14195
        for TASKSPERNODE in `seq 1 $VMS_PER_NODE`; do
            IP=`host $NODE | cut -d ' ' -f 4`
            echo -n "{{" >> $ETCDIR/scalaris.local.cfg
            echo -n $IP | sed s/\\./\,/g >> $ETCDIR/scalaris.local.cfg
            echo -n "},$PORT,service_per_vm}" >> $ETCDIR/scalaris.local.cfg
            if [ "$NODEIDX" -ne "$NR_OF_NODES" ]
            then
                echo "," >> $ETCDIR/scalaris.local.cfg
            fi
            let PORT+=1
            let NODEIDX+=1
        done
    done
    echo "]}." >> $ETCDIR/scalaris.local.cfg

    ## fix mgmt_server
    HEADNODE=`scontrol show hostnames | head -n1`
    echo -n "{mgmt_server, {{" >> $ETCDIR/scalaris.local.cfg
    IP=`host $HEADNODE | cut -d ' ' -f 4`
    echo -n $IP | sed s/\\./\,/g >> $ETCDIR/scalaris.local.cfg
    echo "}, 14195, mgmt_server}}." >> $ETCDIR/scalaris.local.cfg
}

function kill_old_nodes() {
    srun -N$SLURM_JOB_NUM_NODES bash -c "screen -ls | grep Detached | grep scalaris_ | cut -d. -f1 | awk '{print $1}' | xargs -r kill"
}

function start_servers() {
    HEADNODE=`scontrol show hostnames | head -n1`
    TAILNODES=`scontrol show hostnames | tail -n +2`

    for NODE in $TAILNODES; do
        PORT=14195
        YAWSPORT=8000
        for TASKSPERNODE in `seq 1 $VMS_PER_NODE`; do
            srun --nodelist=$NODE -N1 --ntasks-per-node=1 $BINDIR/scalarisctl -n node$PORT -p $PORT -y $YAWSPORT --screen -d -t joining start
            let PORT+=1
            let YAWSPORT+=1
        done
    done
    PORT=14196
    YAWSPORT=8001
    for TASKSPERNODE in `seq 2 $VMS_PER_NODE`; do
        srun --nodelist=$HEADNODE -N1 --ntasks-per-node=1 $BINDIR/scalarisctl -n node$PORT -p $PORT -y $YAWSPORT --screen -d -t joining start
        let PORT+=1
        let YAWSPORT+=1
    done
    srun --nodelist=$HEADNODE -N1 --ntasks-per-node=1 $BINDIR/scalarisctl -n first -p 14195 -y 8000 --screen -d -m -t first start
}

function wait_for_servers_to_start {
    for NODE in `scontrol show hostnames`; do
        RUNNING_NODES=`srun --nodelist=$NODE -N1 --ntasks-per-node=1 epmd -names | grep " at port " | wc -l`
        while [ $RUNNING_NODES -ne $VMS_PER_NODE ]
        do
            RUNNING_NODES=`srun --nodelist=$NODE -N1 --ntasks-per-node=1 epmd -names | grep " at port " | wc -l`
        done
    done
}

module load erlang/$ERLANG_VERSION

fix_known_hosts
kill_old_nodes
start_servers
wait_for_servers_to_start
