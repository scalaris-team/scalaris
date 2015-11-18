#!/bin/bash

# -o: output log file: %j for the job ID, %N for the name of the first executing node
# Change the path of the output logfile

#SBATCH -J bench-script
#SBATCH -N 2
#SBATCH -p CUMU
#SBATCH -A csr
#SBATCH --time=00:10:00
#SBATCH --exclusive

source /usr/share/modules/init/bash
source $(pwd)/config/env.sh

check_compile
print_env
$(pwd)/util/start-scalaris.sh

#############################################
#                                           #
#     Place your commands between here      #
#                                           #
#############################################

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
$(pwd)/util/stop-scalaris.sh
