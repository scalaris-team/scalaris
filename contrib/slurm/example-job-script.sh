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


#############################################
#                                           #
#     and here                              #
#                                           #
#############################################

$(pwd)/scalaris-stop.sh
