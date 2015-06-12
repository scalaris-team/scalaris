#!/bin/bash

ERLANG_VERSIONS="R17B05 R14B04"

mkdir -p $OUTPUTDIR

for NNODES in "2" #  1 2 3 4 5 6 7 8 12 16 24 32
do
    for VMS_PER_NODE in "2" #  1 2 4 8
    do
        for DHT_NODES_PER_VM in "1" #  2 4 8
        do
            for VERSION in "R17B05" # $ERLANG_VERSIONS
            do
                export ERLANG_VERSION=$VERSION
                export SCALARIS_DIR=$SCALARIS_DIR
                export VMS_PER_NODE=$VMS_PER_NODE
                export DHT_NODES_PER_VM=$DHT_NODES_PER_VM
                for ITERATION in `seq 1 20`
                do
                    #sbatch --dependency=singleton --job-name scalaris-benchmark -N $NNODES bench-script.sh
                    sbatch --dependency=singleton --job-name scalaris-benchmark -N $NNODES -o slurm-$ERLANG_VERSION-$NNODES-$VMS_PER_NODE-$DHT_NODES_PER_VM-%j.out bench-script.sh
                done
            done
        done
    done
done
