#!/bin/bash
#SBATCH --job-name=cos568-m2
#SBATCH --output=milestone2.out
#SBATCH --error=milestone2.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=00:30:00
#SBATCH --mail-type=begin,end,fail
#SBATCH --mail-user=aa2279@princeton.edu

module purge
module load boost/1.85.0
export LD_LIBRARY_PATH=/usr/local/boost/1.85.0/lib64:$LD_LIBRARY_PATH
cd /scratch/network/aa2279/COS568-LI-SP26
bash scripts/run_hybrid.sh
