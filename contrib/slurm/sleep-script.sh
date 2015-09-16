#!/bin/bash -l




# -o: output log file: %j for the job ID, %N for the name of the first executing node
# Change the path of the output logfile

#SBATCH -J scalaris
#SBATCH -N 2
#SBATCH -p CSR
#SBATCH -A csr
#SBATCH --exclusive

source /usr/share/modules/init/bash
source $(pwd)/env.sh

#$BINDIR/scalarisctl checkinstallation

$(pwd)/scalaris-start.sh

#############################################
#                                           #
#     Place your commands between here      #
#                                           #
#############################################

echo "Nodelist: $SLURM_NODELIST"
env | grep CPU

collectl -s cdmn -o T -f ~/basho_result/collectl_firstnode.log -i5 -F0 &

SLEEPTIME="365d"
echo "sleeping for $SLEEPTIME, need to cancel manually"
sleep $SLEEPTIME

#############################################
#                                           #
#     and here                              #
#                                           #
#############################################

echo "stopping servers"
$(pwd)/scalaris-stop.sh
echo "stopped servers"
