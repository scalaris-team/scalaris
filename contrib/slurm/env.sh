export ERLANG_VERSION=${ERLANG_VERSION:-R17B05}
export SCALARIS_DIR=${SCALARIS_DIR:-$HOME/scalaris}
export VMS_PER_NODE=${VMS_PER_NODE:-1}
export DHT_NODES_PER_VM=${DHT_NODES_PER_VM:-1}
export ERL_SCHED_FLAGS=${ERL_SCHED_FLAGS:-""}

export ETCDIR=$SCALARIS_DIR/bin
export BINDIR=$SCALARIS_DIR/bin
export BEAMDIR=$SCALARIS_DIR/ebin

export SHUFFLE_NODE_IDS=1
export WATCHDOG_INTERVAL=10

function cleanup() {
    echo "Nodelist of the cancelled job: $SLURM_NODELIST"
    echo -e "Use:
    srun -p CUMU -A csr --nodelist='$SLURM_NODELIST' cleanup.sh"
    echo "to clean up the nodes manually"

    # comment in for automatic cleanup
    # for NODE in $NODELIST; do
        # sbatch --job-name cleanup -p CUMU -A csr --nodelist="$NODE" -o cleanup-%j.out cleanup.sh
    # done
    exit 1
}

function test_foreign_beams() {
    BEAM=$(pgrep beam)
    if [[ -n $BEAM ]]; then
        USER=$(ps -e -o user,comm | grep beam | awk '{print $1}' | sort | uniq | xargs echo)
        echo "There are Erlang VMs from $USER still running, please contact $USER for cleanup:"
        echo "$(ps -e -o user,pid,start_time,comm | awk 'NR == 1 {print} /beam/ {print}')"
        return 1
    fi
    return 0
}

export -f cleanup
export -f test_foreign_beams

trap cleanup SIGTERM

