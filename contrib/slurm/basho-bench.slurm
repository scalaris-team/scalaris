#!/bin/bash -l

################################################################################
#
#   BASHO BENCH SLURM SCRIPT
#
#   Starts a Scalaris ring and sleeps for 365d, canceled through
#   the basho-bench.sh script.
#
################################################################################

#SBATCH -J scalaris
#SBATCH -N 1
#SBATCH -p CUMU
#SBATCH -A csr
#SBATCH --exclusive
#SBATCH --time=00:10:00

# setup Scalaris
source /usr/share/modules/init/bash
source $(pwd)/config/env.sh
print_env
echo "Nodelist: $SLURM_NODELIST"
$(pwd)/util/start-scalaris.sh

# check that the WD (working dir) is set
if [[ -z $WD ]]; then
    echo "WD not set, stopping servers"
    $(pwd)/util/stop-scalaris.sh
    exit 1
fi

# create the lock file
# (indicates successfully started scalaris ring to basho-basho.sh)
echo "scalaris started" > $WD/${SLURM_JOBID}.lock


SLEEPTIME="365d"
echo "sleeping for $SLEEPTIME, need to cancel manually"
sleep $SLEEPTIME

echo "stopping servers"
$(pwd)/util/stop-scalaris.sh
echo "stopped servers"
