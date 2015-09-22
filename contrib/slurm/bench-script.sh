#!/bin/bash

# -o: output log file: %j for the job ID, %N for the name of the first executing node
# Change the path of the output logfile

#SBATCH -J bench-script
#SBATCH -N 2
#SBATCH -p CSR
#SBATCH -A csr
#SBATCH --time=00:10:00
#SBATCH --exclusive

source /usr/share/modules/init/bash
source $(pwd)/env.sh

$(pwd)/scalaris-start.sh

#############################################
#                                           #
#     Place your commands between here      #
#                                           #
#############################################
HEAD=$(git rev-parse --short HEAD)
JOBID=$SLURM_JOB_ID
NO_OF_NODES=$SLURM_JOB_NUM_NODES

echo "HEAD; JOBID; NO_OF_NODES; VMS_PER_NODE; PID; NO_OF_CH"
echo "Git-Head: $HEAD"
echo "Job-Id: $JOBID"
echo "Number of DHT Nodes: $(($NO_OF_NODES*$VMS_PER_NODE)) (Nodes: $NO_OF_NODES; VMs per Node: $VMS_PER_NODE)"

METRICS="{value, {mean_troughput_overall, Mean}} = lists:keysearch(mean_troughput_overall, 1, Res), {value, {avg_latency_overall, Latency}} = lists:keysearch(avg_latency_overall, 1, Res)"
LOGSTRING_INC="io:format('result data inc:~p:~p~n', [Mean, Latency])"
LOGSTRING_QR="io:format('result data qr:~p:~p~n', [Mean, Latency])"

THREADS=1024
ITERATIONS=4
echo "running bench:increment($THREADS, $ITERATIONS)..."
erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "{ok, Res} = rpc:call('first@`hostname -f`', bench, increment, [$THREADS, $ITERATIONS]), $METRICS, $LOGSTRING_INC, halt(0)."

THREADS=2048
ITERATIONS=10
echo "running bench:increment($THREADS, $ITERATIONS)..."
erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "{ok, Res} = rpc:call('first@`hostname -f`', bench, quorum_read, [$THREADS, $ITERATIONS]), $METRICS, $LOGSTRING_QR, halt(0)."

#############################################
#                                           #
#     and here                              #
#                                           #
#############################################

echo "stopping servers"
$(pwd)/scalaris-stop.sh
