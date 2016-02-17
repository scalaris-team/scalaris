export SCALARIS_LOCAL=${SCALARIS_LOCAL:-false}
export SCALARIS_SRC=${SCALARIS_SRC:-$HOME/scalaris}
# exports EPMD=... with to the Erlang version set through ./configure
export $(grep 'EPMD=' $SCALARIS_SRC/bin/scalarisctl)
if [[ $SCALARIS_LOCAL = true ]]; then
    export SCALARIS_DIR="/local/$(whoami)/scalaris"
else
    export SCALARIS_DIR=${SCALARIS_DIR:-$HOME/scalaris}
fi
export VMS_PER_NODE=${VMS_PER_NODE:-1}
export DHT_NODES_PER_VM=${DHT_NODES_PER_VM:-1}
export ERL_SCHED_FLAGS=${ERL_SCHED_FLAGS:-""}
export SCALARISCTL_PARAMS=${SCALARISCTL_PARAMS:-""} # additional params for scalarisctl

export COLLECTL=${COLLECTL:-"false"}

export ETCDIR=$SCALARIS_DIR/bin
export BINDIR=$SCALARIS_DIR/bin
export BEAMDIR=$SCALARIS_DIR/ebin
export COLLECTL_DIR=${COLLECTL_DIR:-"$(pwd)/collectl/"}

# collectl arguments
export COLLECTL_SUBSYSTEMS=${COLLECTL_SUBSYSTEMS:-"-s cmnd"}
export COLLECTL_INTERVAL=${COLLECTL_INTERVAL:-"-i 10"}
export COLLECTL_FLUSH=${COLLECTL_FLUSH:-"-F 0"}

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
    BEAM=$(pgrep -a beam)
    if [[ -n $BEAM ]]; then
        USER=$(ps -e -o user,comm | grep beam | awk '{print $1}' | sort | uniq | xargs echo)
        echo "There are Erlang VMs from $USER still running, please contact $USER for cleanup:"
        echo "$(ps -e -o user,pid,start_time,comm | awk 'NR == 1 {print} /beam/ {print}')"
        echo "pgrep -a beam output: $BEAM"
        echo "sleeping for 30 seconds, then retrying..."
        sleep 30
    fi
    BEAM=$(pgrep -a beam)
    if [[ -n $BEAM ]]; then
        USER=$(ps -e -o user,comm | grep beam | awk '{print $1}' | sort | uniq | xargs echo)
        echo "There are Erlang VMs from $USER still running, please contact $USER for cleanup:"
        echo "$(ps -e -o user,pid,start_time,comm | awk 'NR == 1 {print} /beam/ {print}')"
        echo "pgrep -a beam output: $BEAM"
        return 1
    fi
    return 0
}

function print_env() {
    echo "Nodes: $(($SLURM_JOB_NUM_NODES*$VMS_PER_NODE*$DHT_NODES_PER_VM))($SLURM_JOB_NUM_NODES*$VMS_PER_NODE*$DHT_NODES_PER_VM)"
    echo "erl schedular flags: $ERL_SCHED_FLAGS"
    erl_binary=$(grep "^ERL=" $SCALARIS_DIR/bin/scalarisctl | awk -F= '{print $2}')
    erl_version_file=$($erl_binary -eval 'io:format("~s", [filename:join([code:root_dir(), "releases", erlang:system_info(otp_release), "OTP_VERSION"])]), halt()'  -noshell)
    erl_version=$(cat $erl_version_file)
    echo "Erlang binary: $erl_binary"
    echo "Erlang version: $erl_version"
    HEAD=$(git rev-parse --short HEAD)
    echo "Current git-HEAD: $HEAD"
    echo "Slurm-JOBID: $SLURM_JOB_ID"
    echo "Number of DHT Nodes: $(($SLURM_JOB_NUM_NODES*$VMS_PER_NODE)) (Nodes: $SLURM_JOB_NUM_NODES; VMs per Node: $VMS_PER_NODE)"
    echo "SCALARIS_DIR=$SCALARIS_DIR"
    if [[ -n $WD && -n $NAME ]]; then
        printenv > $WD/$NAME/slurm-${SLURM_JOB_ID}.env
    else
        printenv > slurm-${SLURM_JOB_ID}.env
    fi
}

check_compile(){
    pushd $SCALARIS_DIR >/dev/null
    local res=$(erl -pa contrib/yaws -pa ebin -noinput +B -eval 'R=make:all([noexec]), halt(0).')
    popd >/dev/null
    if [[ -n $res ]]; then
        echo "Scalaris binaries do not match source version:"
        echo $res
        echo "exiting..."
        exit 1
    fi
}

export -f cleanup
export -f test_foreign_beams
export -f print_env
export -f check_compile

trap cleanup SIGTERM

