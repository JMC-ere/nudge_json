#!/bin/sh

WORK_HOME="/home/manager/svc/index/nudge_json"

echo $WORK_HOME

cd $WORK_HOME

echo "first_policy_index.py start"

python $WORK_HOME/src/first_policy_index.py config/connect_info.json update

echo "first_policy_index.py end"