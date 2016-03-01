#!/bin/bash

###############################################################################
# Author: Jens V. Fischer
#
# Sync all crashdumps from the local direcotry to the given subdirectory.
#
# Call:
# 	./collect_crashdumps.sh <jobid> <dir>
#
###############################################################################

jobid=${1?"no jobid"}
# subdir to put the crashdumps in
csub_dir=${2?"no dir"}

# local dir (on the slurm nodes) of the scalaris installation
export SLOCAL_DIR="/local/bzcfisch"

# base dir for where to sync the crash dumps
cbase_dir="/scratch/bzcfisch"

export CDIR="$cbase_dir/$csub_dir"

nodelist=$(echo cumu{01,02}-{00..15})

for node in $nodelist; do
    srun --jobid=$jobid -A csr -p CUMU -N1 --nodelist=$node bash <<'EOF'
echo "$(hostname)"
rsync -ayhx --progress --executability $SLOCAL_DIR/scalaris/ebin/erl_crash.dump $CDIR/$(hostname -s)_crash.dump
echo "rsync finished with exit code $?"
EOF
done

