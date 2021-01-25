#!/bin/bash
# This shell script is to run the check attendance script in Python.

. /etc/profile

. ~/anaconda3/etc/profile.d/conda.sh
conda activate ds_env
cd ~/workshop/algo_trade_fan/
python ~/workshop/algo_trade_fan/routine_etl.py >> ~/workshop/algo_trade_fan/normal_result.log 2>> ~/workshop/algo_trade_fan/error.log
conda deactivate