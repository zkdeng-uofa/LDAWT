#!/bin/bash
number="$1"

for ((i = 2; i <= $number; i++)); do
    bash gen_script.sh $i
done

for ((i = 1; i <= $number; i++)); do
    sbatch LDAWT$i.slurm 
done
