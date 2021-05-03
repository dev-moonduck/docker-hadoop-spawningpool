#!/bin/bash

namedir="/hadoop/dfs/name"
if [ ! -d $namedir ]; then
  echo "Namenode name directory not found: $namedir"
  exit 2
fi


echo "remove lost+found from $namedir"
rm -r $namedir/lost+found

if [ "`ls -A $namedir`" == "" ]; then
  echo "Formatting standby namenode name directory: $namedir"
  $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode -bootstrapStandby
fi

$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode &

$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR --daemon start zkfc &