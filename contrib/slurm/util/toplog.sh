#!/bin/bash

# echo "PID: $$"

# set -x
LOG="$(hostname -s)_top.log"
SLEEP=1

if [[ -z $? ]]; then
    echo "[error] No basedir as first argument, exiting"
    exit 1
else
    BASEDIR=$1
fi


while true; do
    echo "$(date +%y.%m.%d-%H:%M:%S):" >> $BASEDIR/$LOG
    top -bn1 | awk 'NR>6 && NR <12{print $0}' >> $BASEDIR/$LOG
    echo "" >> $BASEDIR/$LOG
    sleep 5
done
