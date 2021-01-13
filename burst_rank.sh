#!/bin/bash
#SBATCH -t 0-03:00
#SBATCH --mem=8G
#SBATCH -o burst_rank.log

source env/bin/activate
python burst_rank.py
deactivate

