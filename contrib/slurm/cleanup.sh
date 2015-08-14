#!/bin/bash

HOSTNAME="$(hostname)"
echo "[$HOSTNAME] $(screen -ls)"
screen -ls | grep Detached | grep scalaris_ | cut -d. -f1 | awk '{print $1}' | xargs -r kill
echo "[$HOSTNAME] killing epmd process..."
pkill epmd
