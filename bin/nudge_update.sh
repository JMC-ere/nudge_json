#!/bin/sh

WORK_HOME="/home/manager/svc/index/nudge_json"

echo $WORK_HOME

cd $WORK_HOME

echo "policy index update start"

python $WORK_HOME/src/make_policy_index.py

echo "policy index update end"
