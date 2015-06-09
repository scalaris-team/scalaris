#!/bin/bash -l

# -o: output log file: %j for the job ID, %N for the name of the first executing node
# Change the path of the output logfile

#SBATCH -J bench-script
#SBATCH -N 2
#SBATCH -p CSR
#SBATCH -A csr
#SBATCH --exclusive

source $(pwd)/env.sh

#$BINDIR/scalarisctl checkinstallation

$(pwd)/scalaris-start.sh

#############################################
#                                           #
#     Place your commands between here      #
#                                           #
#############################################

sleep 15

echo "running bench:increment(10, 500)..."
erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "rpc:call('first@`hostname -f`', bench, increment, [10, 500]), halt(0)."
echo "running bench:quorum_read(10, 5000)..."
erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "rpc:call('first@`hostname -f`', bench, quorum_read, [10, 5000]), halt(0)."

#############################################
#                                           #
#     and here                              #
#                                           #
#############################################

$(pwd)/scalaris-stop.sh
