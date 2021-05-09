#!/bin/bash

su --preserve-environment spark -c "$JAVA_HOME/bin/java -cp /opt/spark/conf/:/opt/spark/jars/*:/etc/hadoop/:/opt/hadoop-3.3.0/share/hadoop/common/lib/*:/opt/hadoop-3.3.0/share/hadoop/common/*:/opt/hadoop-3.3.0/share/hadoop/hdfs/:/opt/hadoop-3.3.0/share/hadoop/hdfs/lib/*:/opt/hadoop-3.3.0/share/hadoop/hdfs/*:/opt/hadoop-3.3.0/share/hadoop/mapreduce/*:/opt/hadoop-3.3.0/share/hadoop/yarn/:/opt/hadoop-3.3.0/share/hadoop/yarn/lib/*:/opt/hadoop-3.3.0/share/hadoop/yarn/* -Xmx2g org.apache.spark.deploy.history.HistoryServer --properties-file /spark_history_server.conf"
