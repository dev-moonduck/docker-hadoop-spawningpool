#!/bin/bash
if [ "$MY_NODE_NUM" = "" ]; then
    exit -1
fi

echo "$MY_NODE_NUM" > $ZOOKEEPER_HOME/data/myid
$ZOOKEEPER_HOME/bin/zkServer.sh start
