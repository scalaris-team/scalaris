#!/bin/bash

###############################################################################
# Author: Jens V. Fischer
#
# Watch for crash reports in the Scalaris log files. Send kill -SIGUSR1 to all
# Scalaris nodes to create a crashdump. Scalaris should run in a local dir,
# otherwise all nodes try to write into the same crashdump.
#
# Call:
# 	./crashwatcher <jobid> <no_of_nodes>
#
###############################################################################


JOBID=$1
NODES=$2

WD="$HOME/bbench"

[[ -z $JOBID ]] && { echo "no jobid, exiting..."; exit 1; }
echo "JOBID: $JOBID"

[[ -z $NODES ]] && { echo "no number of nodes given, exiting..."; exit 1; }
echo "NODES: $NODES"

watch() {
    find $WD -iname scalaris_log4erl\* | xargs grep crash
}

kill_bbench(){
    # kill all remaining basho_bench processes
    ssh bzcfisch@buildbot2.zib.de bash -c "'pkill -f basho_bench'"
    pkill -f basho_bench
}

wait_for_crash() {
    echo -n "waiting for crash notice in Scalaris log files"
    timer=0
    until watch; do
        ((timer++))
        # display status every 5 seconds
        if ((timer%5==0)); then
            echo -ne "."
        fi
        sleep 1
    done
    echo ": ok (${timer}s)"
}

send_sigusr1() {
    echo "sending SIGUSR1"
    srun --jobid=$JOBID -p CUMU -A csr -N $NODES bash <<'EOF'
echo -n "$(hostname). "
ps -e -o user,pid,start_time,comm | grep beam | awk '{print $2}' | xargs -r kill -SIGUSR1
echo "ret: $?"
EOF
}

wait_for_crash
kill_bbench

# sleeptime="10m"
# echo "$(date +%y.%m.%d-%H:%M:%S): sleeping for $sleeptime"
# sleep $sleeptime

send_sigusr1

