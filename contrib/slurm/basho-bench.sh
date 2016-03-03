#!/bin/bash

###############################################################################
# Author: Jens V. Fischer
# Date: 11.11.2015
#
# Running a benchmark series on a Scalaris slurm installation with basho bench.
# The scripts reads the basho-bench.cfg, sets up a Scalaris ring using the slurm
# cluster management tool, using the slurm script collection of Scalaris with
# basho_bench.slurm as slurm script. It then waits for the ring to set up and
# starts multiple basho bench intances ("load # generators") on one or multiple
# machines by calling start-basho-bench.sh.
#
# Call:
# 	./basho-bench.sh
#
# Configuration:
#   All configuration settings, including documentation and default values can
#   be found in basho-bench.cfg.
#   A quick-config section is provided below for overriding values from the
#   configuration file. This is meant for easy manipulation of the most commonly
#   used configuration parameters.
###############################################################################

trap 'trap_cleanup' SIGTERM SIGINT

# QUICK-CONFIG ===============

# Values defined here override settings from the configuration file

# REPETITIONS=2
# DURATION=5
# LOAD_GENERATORS=4
#
# PARTITION="CUMU"
# TIMEOUT=15
# SLEEP1=30
# SLEEP2=30
#
# SCALARIS_LOCAL=true
# COLLECTL=true
#
# # size scalability series (only uncomment 'size' or 'load')
# KIND='size'
# NODES_SERIES="1 2 4 8 16 32"
# VMS_PER_NODE_SERIES="1"
# export ERL_SCHED_FLAGS="+S 32"
# LOAD_LEVEL=5

# vary value sizes
# KIND='value'
# WORKERS_PER_LG=44
# VALUE_SIZES="2 4 8 16 32 64 128 256 512 768 1024 1280 1536 1792 2048"
# NODES=32

# load scalability series
# KIND='load'
# NODES=32
# VMS_PER_NODE=1
# WORKERS_PER_LG_SERIES="1 2 4 8 16 32 64 128 256 512 1024 2048"

# KIND="lgs"
# LOAD_GENERATORS_SERIES="4 6 8 10 12 15"
# WORKERS="8400"
# NODES=32
# VMS_PER_NODE=4
# export ERL_SCHED_FLAGS="+S 8"

#=============================

main() {
    source $(pwd)/config/basho-bench.cfg
    check_wdir
    check_result_dir
    setup_logging
    print_env
    check_compile

    if [[ $KIND == "size" ]]; then
        main_size
    elif [[ $KIND == "load" ]]; then
        main_load
    elif [[ $KIND == "value" ]]; then
        main_value
    elif [[ $KIND == "lgs" ]]; then
        main_lgs
    else
        log error "Unknown kind of benchmark, exiting"
        exit 1
    fi
}

main_lgs(){
    for LOAD_GENERATORS in $LOAD_GENERATORS_SERIES; do
        WORKERS_PER_LG=$((WORKERS/LOAD_GENERATORS))
        log info "starting load benchmark with $LOAD_GENERATORS LOAD_GENERATORS"
        log info "WORKERS=$WORKERS"
        log info "WORKERS_PER_LG=$WORKERS_PER_LG"

        PREFIX="lgs$(printf "%04i" $LOAD_GENERATORS)"
        repeat_benchmark
    done
}


main_value() {
    for VALUE_SIZE in $VALUE_SIZES; do
        local value=$(printf "%04i" $VALUE_SIZE)
        PREFIX="value$value"
        log info "starting value benchmark with $VALUE_SIZE"
        repeat_benchmark
    done
}

main_size(){
    for NODES in $NODES_SERIES; do
        if ((NODES==1)); then
            NODELIST=""
        elif ((NODES==2)); then
            NODELIST="cumu01-00,cumu02-00"
        else
            half=$((NODES/2-1))
            NODELIST="cumu01-[00-$half],cumu02-[00-$half]"
        fi

        for VMS_PER_NODE in $VMS_PER_NODE_SERIES; do
            local ringsize=$((NODES*VMS_PER_NODE*DHT_NODES_PER_VM))
            WORKERS=$((ringsize*LOAD_LEVEL))
            WORKERS_PER_LG=$((WORKERS/LOAD_GENERATORS))
            log info "RINGSIZE=$ringsize"
            log info "WORKERS=$WORKERS"
            log info "WORKERS_PER_LG=$WORKERS_PER_LG"

            ringsize=$(printf "%04i" $ringsize)
            PREFIX="size$ringsize"

            repeat_benchmark
        done
    done
}

main_load(){
    for WORKERS_PER_LG in $WORKERS_PER_LG_SERIES; do
        WORKERS=$((WORKERS_PER_LG*LOAD_GENERATORS))
        log info "WORKERS=$WORKERS"
        log info "WORKERS_PER_LG=$WORKERS_PER_LG"

        WORKERS=$(printf "%04i" $WORKERS)
        PREFIX="load$WORKERS"
        log info "starting load benchmark with $WORKERS ($WORKERS_PER_LG*$LOAD_GENERATORS)"

        repeat_benchmark
    done
}

repeat_benchmark() {
    for run in $(seq 1 $REPETITIONS); do

        NAME="${PREFIX}-r$run"
        mkdir ${WD}/${NAME}
        setup_directories
        create_result_dir

        # setup Scalaris log path
        SLOGPATH=${SLOGPATH/?NAME/$NAME}  # replace "?NAME" with "$NAME"
        SCALARISCTL_PARAMS="$SLOGPATH $SCTL_PARAMS"  # Prepend log path
        echo ${!SCALARISCTL_PARAMS@}=$SCALARISCTL_PARAMS

        COLLECTL_DIR=$WD/$NAME/collectl
        echo ${!COLLECTL_DIR@}=$COLLECTL_DIR

        log info "starting repetition $run..."
        [[ $COLLECTL = true ]] && start_collectl
        [[ $TOPLOG = true ]] && start_toplog
        start_scalaris

        wait_for_scalaris_startup
        build_hostlist

        test_ring
        run_bbench
        test_ring

        stop_scalaris
        rm_lockfile

        log info "sleeping for $SLEEP1 seconds"; sleep $SLEEP1
        [[ $COLLECTL = true ]] && stop_collectl
        [[ $TOPLOG = true ]] && stop_toplog
    done

    if (( SLEEP2 > 0 )); then
        log info "sleeping for $SLEEP2 seconds"
        sleep $SLEEP2
    fi
    collect_bbench_results

}


#=====================
# FUNCTIONS
#=====================

check_wdir() {
    # check if WD exists
    if [[ ! -d $WD ]]; then
        mkdir -p $WD
    else
        # check if WD is empty
        if [ "$(ls $WD)" ]; then
            log info "Working directory ($WD) is not empty, containing the following files/dirs:"
            ls -l1 $WD
            read -p "Delete all files? " -n 1 -r
            echo    # move to a new line
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                rm -r $WD/*
            else
                log error "aborting..."
                exit 1
            fi
        fi
    fi
}

setup_logging(){
    LOGFILE="$WD/bbench-suite-$(date +%y.%m.%d-%H:%M:%S).log"
    log info "writing output also to $LOGFILE"
    # w/o -i option to tee, signal trapping does NOT work!
    exec &>> >(tee -i $LOGFILE)
}

setup_directories(){
    if [[ $COLLECTL = true && ! -d $WD/$NAME/collectl ]]; then
        mkdir -p $WD/$NAME/collectl
    fi
}


print_env(){
    echo KIND=$KIND
    if [[ $KIND == "load" ]]; then
        echo RINGSIZE=$((NODES*VMS_PER_NODE*DHT_NODES_PER_VM))
        echo NODES=$NODES
        echo VMS_PER_NODE=$VMS_PER_NODE
        echo WORKERS_PER_LG_SERIES=$WORKERS_PER_LG_SERIES
    elif [[ $KIND == "size" ]]; then
        echo NODES_SERIES=$NODES_SERIES
        echo VMS_PER_NODE_SERIES=$VMS_PER_NODE_SERIES
        echo LOAD_LEVEL=$LOAD_LEVEL
    elif [[ $KIND == "value" ]]; then
        echo RINGSIZE=$((NODES*VMS_PER_NODE*DHT_NODES_PER_VM))
        echo WORKERS_PER_LG=$WORKERS_PER_LG
        echo VALUE_SIZES=$VALUE_SIZES
    fi
    echo "ERL_SCHED_FLAGS=$ERL_SCHED_FLAGS"
    echo TIMEOUT=$TIMEOUT
    echo REPETITIONS=$REPETITIONS
    echo DURATION=$DURATION
    echo LOAD_GENERATORS=$LOAD_GENERATORS
    echo LG_HOSTS=${LG_HOSTS[@]}
    echo SLEEP1=$SLEEP1
    echo SLEEP2=$SLEEP2
    echo "COLLECTL=$COLLECTL"
    echo "PARTITION=$PARTITION"
    echo "VALUE_SIZE=$VALUE_SIZE"
}

check_compile(){
    pushd $SCALARIS_DIR >/dev/null
    local res=$(erl -pa contrib/yaws -pa ebin -noinput +B -eval 'R=make:all([noexec]), halt(0).')
    popd >/dev/null
    if [[ -n $res ]]; then
        log error "Scalaris binaries do not match source version:"
        echo $res
        exit 1
    fi
}

log(){
    local level=$1
    local message=$2
    printf "%s %s\n" "$(tag $level)" "$message"
}

tag(){
    local level=$1
    printf "[bbench] %s  [%s]" "$(date +%H:%M:%S)" "$level"
}

start_collectl() {
    export COLLECTL_SUBSYSTEMS
    export COLLECTL_INTERVAL
    export COLLECTL_FLUSH
    # start collectl at the load generators
    for host in ${LG_HOSTS[@]}; do
        log info "starting collectl on $host"
        if [[ $(hostname -f) = $host ]]; then
            collectl $COLLECTL_SUBSYSTEMS $COLLECTL_INTERVAL $COLLECTL_FLUSH -f $WD/$NAME/collectl/lg_$host 2>/dev/null &
        else
            ssh $host collectl $COLLECTL_SUBSYSTEMS $COLLECTL_INTERVAL $COLLECTL_FLUSH -f $WD/$NAME/collectl/lg_$host 2>/dev/null &
        fi
    done
}

stop_collectl(){
    # stop collectl on load generators (collectl on slurm nodes are killed by the watchdog)
    for host in ${LG_HOSTS[@]}; do
        log info "killing collectl on $host"
        if [[ $(hostname -f) = $host ]]; then
            pkill -f lg_$host
        else
            ssh $host pkill -f lg_$host
        fi
    done
}

start_toplog() {
    # start toplog at the load generators
    for host in ${LG_HOSTS[@]}; do
        log info "starting toplog on $host"
        if [[ $(hostname -f) = $host ]]; then
            $SCALARIS_DIR/contrib/slurm/util/toplog.sh "$WD/$NAME" &
        else
            ssh $host $SCALARIS_DIR/contrib/slurm/util/toplog.sh "$WD/$NAME" &
        fi
    done
}

stop_toplog(){
    # stop toplog on load generators
    for host in ${LG_HOSTS[@]}; do
        log info "killing toplog on $host"
        if [[ $(hostname -f) = $host ]]; then
            pkill -f toplog.sh
        else
            ssh $host pkill -f toplog.sh
        fi
    done
}

# Setup up a scalaris ring on with slurm on cumulus
start_scalaris() {
    log info "starting scalaris..."
    # setup environment
    [[ -n $VMS_PER_NODE ]] && export VMS_PER_NODE
    [[ -n $WATCHDOG_INTERVAL ]] && export WATCHDOG_INTERVAL
    [[ -n $DHT_NODES_PER_VM ]] && export DHT_NODES_PER_VM
    [[ -n $SHUFFLE_NODE_IDS ]] && export SHUFFLE_NODE_IDS
    [[ -n $WD ]] && export WD
    [[ -n $COLLECTL ]] && export COLLECTL
    [[ -n $COLLECTL_DIR ]] && export COLLECTL_DIR
    [[ -n $SCALARIS_LOCAL ]] && export SCALARIS_LOCAL
    [[ -n $SCALARISCTL_PARAMS ]] && export SCALARISCTL_PARAMS
    [[ -n $NAME ]] && export NAME
    [[ -n $ERL_SCHED_FLAGS ]] && export ERL_SCHED_FLAGS

    # start sbatch command and capture output
    # the ${var:+...} expands only, if the variable is set and non-empty
    RET=$( sbatch -A csr -o $WD/$NAME/slurm-%j.out \
            ${PARTITION:+-p $PARTITION} \
            ${NODES:+-N $NODES} \
            ${NODELIST:+ --nodelist=$NODELIST} \
            ${TIMEOUT:+ -t $TIMEOUT} \
            basho-bench.slurm
         )

    # get the job id from the output of sbatch
    REGEX="Submitted batch job ([[:digit:]]*)"
    if [[ $RET =~ $REGEX ]]; then
        SLURM_JOBID=${BASH_REMATCH[1]}
    else
        exit 1
    fi
    local nodes="$(($NODES*$VMS_PER_NODE*$DHT_NODES_PER_VM)) ($NODES*$VMS_PER_NODE*$DHT_NODES_PER_VM)"
    log info "submitted batch job $SLURM_JOBID to start scalaris with $nodes"
}

wait_for_scalaris_startup() {
    LOCKFILE="${WD}/${SLURM_JOBID}.lock"
    echo -n "$(tag info) waiting for scalaris to start"
    timer=0
    until [[ -e $LOCKFILE ]]; do
        ((timer++))
        # display status every 5 seconds
        if ((timer%5==0)); then
            echo -ne "."
        fi
        sleep 1
    done
    echo ": ok (${timer}s)"
}

test_ring() {
    local retries=$1
    local res=0
    [[ -z "$retries" ]] && retries=0
    local ringsize=$((NODES*VMS_PER_NODE*DHT_NODES_PER_VM))
    log info "testing ring"
    erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval \
        "A = rpc:call($FIRST, admin, number_of_nodes, []),
         case A of
             $ringsize -> halt(0);
             _ -> io:format('number_of_nodes: ~p~n', [A]), halt(1)
         end."
    res=$((res+=$?))
    erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval \
        "A = rpc:call($FIRST, admin, check_ring, []),
         case A of
             ok -> halt(0);
             Error -> io:format('check_ring: ~p~n', [Error]), halt(1)
         end."
    res=$((res+=$?))
    erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval \
        "A = rpc:call($FIRST, admin, check_ring_deep, []),
         case A of
             ok -> halt(0);
             Error -> io:format('check_ring_deep: ~p~n', [Error]), halt(1)
         end."
    res=$((res+=$?))


    if  [[ $res -eq 0 ]]; then
        log info "testing ring was successful"
    else
        if (( retries++ >= 2 )); then
            log error "test_ring failed, after $retries retries. Aborting..."
            shutdown
            kill_bbench
            exit 1
        else
            local sleeptime=20
            log error "testing ring failed, retrying in $sleeptime seconds..."
            sleep $sleeptime
            test_ring $retries
        fi
    fi
}

stop_scalaris(){
    log info "stopping scalaris"
    scancel $SLURM_JOBID
}

build_hostlist() {
    local counter=0
    declare -a hosts
    NODELIST=$(scontrol show job $SLURM_JOBID | grep " NodeList" | awk -F= '{print $2}')
    for host in $(scontrol show hostnames $NODELIST); do
        counter=$(($counter+1))
        max_port=$((14194+VMS_PER_NODE))
        for port in $(seq 14195 $max_port); do
            if (( ${#hosts[@]} == 0 )); then
                hosts+=("'first@${host}.zib.de'")
            else
                hosts+=("'node${port}@${host}.zib.de'")
            fi
        done
    done
    FIRST=${hosts[0]}
    HOSTLIST=$(join "${hosts[@]}")
}

join() {
    local IFS=","
    echo "$*"
}

write_config() {
    local max_key=$((NODES*2**17))
    local config=${WD}/${NAME}/lg${PARALLEL_ID}.config
    cat >  $config <<EOF
{rng_seed, $RANDOM_SEED}.
{mode, $MODE}.
{duration, $DURATION}.
{concurrent, $WORKERS_PER_LG}.
{operations, [{put,2}, {get, 8}]}.
{driver, basho_bench_driver_scalaris}.
{key_generator, {int_to_str, {uniform_int, $max_key}}}.
%%{key_generator, {int_to_str, {uniform_int, 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF}}}.
%% size in Bytes
{value_generator, {fixed_bin, $VALUE_SIZE}}.
{scalarisclient_mynode, ['benchclient${PARALLEL_ID}']}.
{scalarisclient_cookie, 'chocolate chip cookie'}.

%{remote_nodes, [{'buildbot2.zib.de', 'nodeB'}]}.
%{distribute_work, true}.
{report_interval, 5}.
{log_level, info}.

{scalarisclient_nodes, [$HOSTLIST]}.
EOF
}

run_bbench() {
    declare -a lg_pids # the process id's of the load generators
    local no_of_hosts=${#LG_HOSTS[*]}
    local c # counter for indexing the LG_HOSTS array

    for i in $(seq 1 $LOAD_GENERATORS); do
        PARALLEL_ID=$i
        RANDOM_SEED="{$((7*$i)), $((11*$i)), $((5*$i))}"
        write_config

        # build args. The ${var:+...} expands only, if the variable is set and non-empty
        local arg1=${SLURM_JOBID:+"--jobid=$SLURM_JOBID"}
        local arg2=${PARALLEL_ID:+"--parallel_id=$PARALLEL_ID"}
        local arg3=${WD:+"--wd=$WD"}
        local arg4=${NAME:+"--name=$NAME"}
        local arg5=${BBENCH_DIR:+"--bbdir=$BBENCH_DIR"}
        local arg6=${RESULT_DIR:+"--rdir=$RESULT_DIR"}
        declare -a args=($arg1 $arg2 $arg3 $arg4 $arg5 $arg6)

        # get current host and (post)increment counter
        host=${LG_HOSTS[$((c++ % no_of_hosts))]}

        if [[ $(hostname -f) = $host ]]; then
            $SCALARIS_DIR/contrib/slurm/util/start-basho-bench.sh ${args[@]} &
            lg_pids[$i]=$!
        else
            # using -t (pseudo-tty allocation) allows to terminate children of the
            # ssh cmd at the remote node through kill the ssh process at the local node
            ssh -t -t $host $SCALARIS_DIR/contrib/slurm/util/start-basho-bench.sh ${args[@]} &
            lg_pids[$i]=$!
        fi
    done

    # wait for load generators to finish
    for pid in "${lg_pids[@]}"; do
        wait $pid
    done
}

check_result_dir() {
    for host in ${LG_HOSTS[@]}; do
        local res=0

        if [[ $(hostname -f) = $host ]]; then
            $SCALARIS_DIR/contrib/slurm/util/checkdir.sh $RESULT_DIR
            res=$((res+=$?))
        else
            ssh -t -t $host "$SCALARIS_DIR/contrib/slurm/util/checkdir.sh $RESULT_DIR"
            res=$((res+=$?))
        fi

        if [[ $res -ne 0 ]]; then
            log error "Result dir ($RESULT_DIR) on $host not empty, aborting"
            exit 1
        fi
    done

}

create_result_dir() {
    for host in ${LG_HOSTS[@]}; do
        log info "creating result dir on $host"
        if [[ $(hostname -f) = $host ]]; then
            mkdir -p $RESULT_DIR/$NAME
        else
            ssh -t -t $host "bash -c \"mkdir -p $RESULT_DIR/$NAME\""
        fi
    done
}

collect_bbench_results() {
    for host in ${LG_HOSTS[@]}; do
        log info "collecting bbench results from $host"
        if [[ $(hostname -f) = $host ]]; then
            rsync -ayhx --progress $RESULT_DIR/ $WD/
            if [[ $? == 0 ]]; then
                log info "deleting $RESULT_DIR/$PREFIX* on $host"
                rm -r $RESULT_DIR/$PREFIX*
            fi
        else
            ssh -t -t $host "bash -c \"rsync -ayhx --progress $RESULT_DIR/ $WD/\""
            if [[ $? == 0 ]]; then
                log info "deleting $RESULT_DIR/$PREFIX* on $host"
                ssh -t -t $host "bash -c \"rm -r $RESULT_DIR/$PREFIX*\""
            fi
        fi
    done
}

kill_bbench(){
    log info "killing bbench..."

    # kill all load generators (or their ssh processes respectively)
    for pid in "${lg_pids[@]}"; do
        kill $pid
    done
}

trap_cleanup(){
    log info "received SIGTERM, cleaning up..."
    kill_bbench
    shutdown
    # kill all remaining children of the current script
    PGID=$(ps -o pgid= $$ | grep -o [0-9]*)
    setsid kill -9 -- -$PGID
    exit 1
}

shutdown(){
    stop_scalaris
    [[ $COLLECTL = true ]] && stop_collectl
    [[ $TOPLOG = true ]] && stop_toplog
    collect_bbench_results
    rm_lockfile
}

rm_lockfile() {
    # remove lockfile
    local lockfile="${WD}/${SLURM_JOBID}.lock"
    rm -f $lockfile
}

main
