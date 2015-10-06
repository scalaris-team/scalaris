#!/bin/bash

# Script to clean up the remaining processes from Scalaris on a slurm node after
# a job was canceled

source $(pwd)/env.sh

HOSTNAME="$(hostname)"

function test_success() {
    if [ $? -eq 0 ]; then
        echo "ok"
    else
        echo "failed"
    fi
}

# find all the screen sessions running a scalaris node and kill them
SCALARIS_SESSIONS=$(screen -ls | grep Detached | grep scalaris_ | awk '{print $1}')
if [[ -z $SCALARIS_SESSIONS ]]; then
    echo "[$HOSTNAME] no Scalaris sessions of $(whoami) running"
else
    echo "[$HOSTNAME] Running Scalaris sessions:"
    for SESSION in $SCALARIS_SESSIONS; do
        echo -e "\t$SESSION"
    done
    echo -n "[$HOSTNAME] killing scalaris session... "
    screen -ls | grep Detached | grep scalaris_ | cut -d. -f1 | awk '{print $1}' | xargs kill
    test_success
fi

# kill the epmd
EPMD=$(pgrep epmd)
if [[ -z $EPMD ]]; then
    echo "[$HOSTNAME] no epmd running"
else
    echo -n "[$HOSTNAME] epmd running, killing the epmd process... "
    kill $EPMD
    test_success
fi

test_foreign_beams




