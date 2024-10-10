#!/bin/bash
#SBATCH --job-name=taskvine           
#SBATCH --output=result.out           
#SBATCH --account=nirav
#SBATCH --partition=standard           
#SBATCH --nodes=1
#SBATCH --ntasks=2                    
#SBATCH --time=01:00:00               

# Load any necessary modules
module load python/3.9
source ~/.bashrc
conda init bash
conda init
conda activate taskvine

# Execute the Python script or command
vine_submit_workers -T slurm -p "--cpus-per-task=4 --time=1:00:00" -t 01:00:00 10.140.86.137 9124 10

