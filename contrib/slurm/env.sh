export ERLANG_VERSION=${ERLANG_VERSION:-R17B05}
export SCALARIS_DIR=${SCALARIS_DIR:-$HOME/scalaris}
export VMS_PER_NODE=${VMS_PER_NODE:-1}
export DHT_NODES_PER_VM=${DHT_NODES_PER_VM:-1}

export ETCDIR=$SCALARIS_DIR/bin
export BINDIR=$SCALARIS_DIR/bin
export BEAMDIR=$SCALARIS_DIR/ebin

export SHUFFLE_NODE_IDS=1

function cleanup() {
    NODES=$(scontrol show hostnames)
    NODELIST=$(echo $NODES | sed 's/ /,/g')
    echo "Nodelist of the cancelled job: $NODELIST"
    echo -e "Use:
    srun -p CSR -A csr --nodelist=\"$NODELIST\" cleanup.sh"
    echo "to clean up the nodes manually"

    # comment in for automatic cleanup
    # for NODE in $NODELIST; do
        # sbatch --job-name cleanup -p CSR -A csr --nodelist="$NODE" -o cleanup-%j.out cleanup.sh
    # done
    exit 1
}

trap cleanup SIGTERM


