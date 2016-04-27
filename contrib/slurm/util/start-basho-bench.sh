#!/bin/bash

main() {
    parse_args "$@"

    # prepend tag to every output (stdout and stderr)
    TAG="[lg_${PARALELL_ID}]"
    exec &> >( while read line; do echo "${TAG} ${line}"; done)

    # check_running
    start_bbench
}

parse_args() {
    for i in "$@"; do
        case $i in
            --jobid=*)
                # get only the part after the "=" with bash substring removal
                JOBID="${i#*=}"
                shift
                ;;
            --wd=*)
                WD="${i#*=}"
                shift
                ;;
            --rdir=*)
                RESULT_DIR="${i#*=}"
                shift
                ;;
            --bbdir=*)
                BBENCH_DIR="${i#*=}"
                shift
                ;;
            --vms_per_node=*)
                VMS_PER_NODE="${i#*=}"
                shift
                ;;
            --parallel_id=*)
                PARALELL_ID="${i#*=}"
                shift
                ;;
            --name=*)
                BBENCH_NAME="${i#*=}"
                shift
                ;;
            *)
                echo "unknown option, exiting" # unknown option
                exit 1
                ;;
        esac
    done
    [[ -z $JOBID ]] && { log error "--jobid not specified"; res=$((ret+=1)); }
    [[ -z $WD ]] && { log error "--wd not specified"; res=$((ret+=1)); }
    [[ -z $RESULT_DIR ]] && { log error "--rdir not specified"; res=$((ret+=1)); }
    [[ -z $BBENCH_DIR ]] && { log error "--bbdir not specified"; res=$((ret+=1)); }
    [[ -z $BBENCH_NAME ]] && { log error "name not specified"; res=$((ret+=1)); }
    [[ -z $PARALELL_ID ]] && { log error "--parallel_id not specified"; res=$((ret+=1)); }
    ((res>0)) && { log error "parameter missing, exiting"; exit 1; }
}

check_running(){
    # check for running bbench instances
    ppids=$(pgrep -af basho_bench | grep -v $JOBID)
    if [[ -n $ppids ]]; then
        echo "[error] benchmarks still running, exiting..."
        pgrep -af basho_bench | grep -v $JOBID
        exit 1
    fi
}

start_bbench() {
    local config="${WD}/${BBENCH_NAME}/lg${PARALELL_ID}.config"
    $BBENCH_DIR/basho_bench -n lg${PARALELL_ID} -N vm${PARALELL_ID} \
        -C 'chocolate chip cookie' --results-dir $RESULT_DIR/$BBENCH_NAME $config
}

log(){
    local level=$1
    local message=$2
    printf "[bbench] %s  [%s] %s\n" "$(date +%H:%M:%S)" "$level" "$message"
}

main "$@"

