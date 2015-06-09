#!/bin/bash

ERLANG_VERSIONS="R17B05 R14B04"
SCALARIS_DIR=$HOME/scalaris

for NNODES in "2" #  1 2 3 4 5 6 7 8 12 16 24 32
do
    for VMS_PER_NODE in "2" #  1 2 4 8
    do
        for DHT_NODES_PER_VM in "1" #  2 4 8
        do
            for VERSION in "R17B05" # $ERLANG_VERSIONS
            do
                sbatch --dependency=singleton --export=ERLANG_VERSION=$VERSION,SCALARIS_DIR=$SCALARIS_DIR,VMS_PER_NODE=$VMS_PER_NODE,DHT_NODES_PER_VM=$DHT_NODES_PER_VM --job-name scalaris-benchmark -N $NNODES bench-script.sh
            done
        done
    done
done
