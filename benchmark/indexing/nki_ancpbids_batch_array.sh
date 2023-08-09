#!/bin/bash

#SBATCH --job-name=index_benchmark
#SBATCH --partition=RM-shared
#SBATCH --ntasks=4
#SBATCH --mem=8000
#SBATCH --time=04:00:00
#SBATCH --array=2-10
#SBATCH --account=med220004p

# Activate conda
eval "$(conda shell.bash hook)"

method=ancpbids
dataset="NKI-RS"
bids_dir="/ocean/projects/med220004p/shared/data_raw/NKI-RS/dataset"
workers=1
trial_id=$(printf '%03d' $SLURM_ARRAY_TASK_ID)

out="results/ds-${dataset}_method-${method}_workers-${workers}_trial-${trial_id}.json"

# Activate environment
source envs/${method}/bin/activate
which python

echo python benchmark_indexing.py -w $workers $method $bids_dir $out
python benchmark_indexing.py -w $workers $method $bids_dir $out
