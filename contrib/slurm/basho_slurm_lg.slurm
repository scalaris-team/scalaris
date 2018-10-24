#!/bin/bash -l

# Use this sbatch script to start the bash-bench benchmarks whilst using
# slurm nodes for load generation (calling basho-bench.sh directly will use
# external load generators as defined by config/basho-bench.cfg)

#SBATCH -J load_gen_basho
#SBATCH -p CUMU
#SBATCH -N 1
#SBATCH -A csr
#SBATCH -t 10:00:00
#SBATCH --exclusive
#SBATCH -o example.out

# override of parameters in config/basho-bench.cfg
export LOAD_GENERATORS=1 # load generator are distributed over nodelist

export KIND='load'
export DURATION=2
export NODES=3 # number of scalaris nodes
export VMS_PER_NODE=1
export WORKERS_PER_LG_SERIES="1 2 4 8 16 32 64 128 256 512 1024 2028"
export OPERATIONS_SERIES="[{put,0},{get,1}] [{put,1},{get,19}] [{put,1},{get,9}] [{put,1},{get,1}] [{put,1},{get,0}]"

export WD="$CUMUSCRATCH/$USER/example_work_dir_dir"
export RESULT_DIR="/local/$USER/example_local_dir"


$(pwd)/basho-bench.sh
