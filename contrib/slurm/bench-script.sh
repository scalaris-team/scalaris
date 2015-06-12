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

METRICS="{value, {mean_troughput_overall, Mean}} = lists:keysearch(mean_troughput_overall, 1, Res), {value, {avg_latency_overall, Latency}} = lists:keysearch(avg_latency_overall, 1, Res)"
LOGSTRING_INC="io:format('result data inc:~p:~p~n', [Mean, Latency])"
LOGSTRING_QR="io:format('result data qr:~p:~p~n', [Mean, Latency])"

echo "running bench:increment(10, 500)..."
erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "{ok, Res} = rpc:call('first@`hostname -f`', bench, increment, [10, 500]), $METRICS, $LOGSTRING_INC, halt(0)."
echo "running bench:quorum_read(10, 5000)..."
erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "{ok, Res} = rpc:call('first@`hostname -f`', bench, quorum_read, [10, 5000]), $METRICS, $LOGSTRING_QR, halt(0)."

#############################################
#                                           #
#     and here                              #
#                                           #
#############################################

$(pwd)/scalaris-stop.sh
