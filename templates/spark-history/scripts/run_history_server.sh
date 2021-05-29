#!/bin/bash

#su --preserve-environment spark -c "$JAVA_HOME/bin/java -cp /opt/spark/conf/:/opt/spark/jars/*:/etc/hadoop/:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs/:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn/:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/* -Xmx2g org.apache.spark.deploy.history.HistoryServer --properties-file /spark_history_server.conf"
su --preserve-environment spark -c "$JAVA_HOME/bin/java -cp $SPARK_HOME/conf/:$SPARK_HOME/jars/* -Xmx2g org.apache.spark.deploy.history.HistoryServer --properties-file $SPARK_HOME/conf/history_server.conf"
