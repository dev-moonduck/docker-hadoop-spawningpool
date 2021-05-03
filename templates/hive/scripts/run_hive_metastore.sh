#!/bin/bash

hadoop fs -mkdir -p    /tmp
hadoop fs -mkdir -p    /user/hive/warehouse
hadoop fs -chmod g+w   /tmp
hadoop fs -chmod g+w   /user/hive/warehouse

$HIVE_HOME/bin/schematool -dbType postgres -initSchema

su --preserve-environment hive -c "JAVA_HOME=$JAVA8_HOME cd $HIVE_HOME/bin && ./hive --service metastore"
