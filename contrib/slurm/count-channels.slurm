#!/bin/bash -l

# -o: output log file: %j for the job ID, %N for the name of the first executing node
# Change the path of the output logfile

#SBATCH -J scalaris
#SBATCH -N 2
#SBATCH -p CSR
#SBATCH -A csr
#SBATCH --exclusive

source /usr/share/modules/init/bash
source $(pwd)/config/env.sh

#$BINDIR/scalarisctl checkinstallation

echo $(date)
$(pwd)/util/start-scalaris.sh

#############################################
#                                           #
#     Place your commands between here      #
#                                           #
#############################################

echo "Nodelist: $SLURM_NODELIST"
sleep 3
HEAD=$(git rev-parse --short HEAD)
JOBID=$SLURM_JOB_ID
NO_OF_NODES=$SLURM_JOB_NUM_NODES
echo "current HEAD: $HEAD"

for i in {1..8}; do
    KEYS=1
    ITERS=1
    echo "$(date) running bench:increment($KEYS, $ITERS)..."
    METRICS="{value, {mean_troughput_overall, Mean}} = lists:keysearch(mean_troughput_overall, 1, Res), {value, {avg_latency_overall, Latency}} = lists:keysearch(avg_latency_overall, 1, Res)"
    LOGSTRING_INC="io:format('result data inc:~p:~p~n', [Mean, Latency])"
    erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "{ok, Res} = rpc:call('first@`hostname -f`', bench, increment, [$KEYS, $ITERS]), $METRICS, $LOGSTRING_INC, halt(0)."
    sleep 1
done

sleep 3

echo "$(date) no of channels"

echo "HEAD; JOBID; NO_OF_NODES; VMS_PER_NODE; PID; NO_OF_CH"
# erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "N = rpc:call('first@`hostname -f`', comm_stats, get_no_of_ch, []), io:format('number of channels: ~w~n', [N]), halt(0)."
erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "{no_of_channels, CommServer, NoOfCh} = rpc:call('first@`hostname -f`', comm_stats, get_no_of_ch, []), io:format('$HEAD; $JOBID; $NO_OF_NODES; $VMS_PER_NODE; ~w; ~w;~n', [CommServer, NoOfCh]), halt(0)."
PORT=14196
for TASKSPERNODE in `seq 2 $VMS_PER_NODE`; do
    erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "{no_of_channels, CommServer, NoOfCh} = rpc:call('node$PORT@`hostname -f`', comm_stats, get_no_of_ch, []), io:format('$HEAD; $JOBID; $NO_OF_NODES; $VMS_PER_NODE; ~w; ~w;~n', [CommServer, NoOfCh]), halt(0)."
    let PORT+=1
done

TAILNODES=`scontrol show hostnames | tail -n +2`
for NODE in $TAILNODES; do
    PORT=14195
    for TASKSPERNODE in `seq 1 $VMS_PER_NODE`; do
        # erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "N = rpc:call('node$PORT@$NODE.zib.de', comm_stats, get_no_of_ch, []), io:format('number of channels: ~w~n', [N]), halt(0)."
        erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "{no_of_channels, CommServer, NoOfCh} = rpc:call('node$PORT@$NODE.zib.de', comm_stats, get_no_of_ch, []), io:format('$HEAD; $JOBID; $NO_OF_NODES; $VMS_PER_NODE; ~w; ~w;~n', [CommServer, NoOfCh]), halt(0)."
        let PORT+=1
    done
done


#############################################
#                                           #
#     and here                              #
#                                           #
#############################################

echo "stopping servers"
$(pwd)/util/stop-scalaris.sh
echo "stopped servers"
