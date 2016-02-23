#!/bin/bash -l

# -o: output log file: %j for the job ID, %N for the name of the first executing node
# Change the path of the output logfile

#SBATCH -J bench-script
#SBATCH -N 2
#SBATCH -p CUMU
#SBATCH -A csr
#SBATCH -t 00:03:00
#SBATCH --exclusive

source $(pwd)/config/env.sh

#$BINDIR/scalarisctl checkinstallation
check_compile
$(pwd)/util/start-scalaris.sh

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

$(pwd)/util/stop-scalaris.sh
