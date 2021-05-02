#!/bin/bash

# common functions
source /scripts/functions.sh

PRERUN_PATH=/prerun
POSTRUN_PATH=/postrun
EXTRA_CONFIG_PATH=/config

for script in `ls $PRERUN_PATH/* 2> /dev/null`; do
    echo "Running $script before entrypoint"
    /bin/bash $script
done

configure /etc/hadoop/core-site.xml core CORE_CONF
configure /etc/hadoop/hdfs-site.xml hdfs HDFS_CONF
configure /etc/hadoop/yarn-site.xml yarn YARN_CONF
configure /etc/hadoop/httpfs-site.xml httpfs HTTPFS_CONF
configure /etc/hadoop/kms-site.xml kms KMS_CONF
configure /etc/hadoop/mapred-site.xml mapred MAPRED_CONF

for script in `ls $EXTRA_CONFIG_PATH/* 2> /dev/null`; do
    echo "Running $script to run extra config set script"
    /bin/bash $script
done

python3 /scripts/agent.py /scripts 3333 &

exec "$@"
