#!/bin/bash
### TODO: Make it run on spark user(not root)
$JAVA_HOME/bin/java -cp /opt/spark/conf/:/opt/spark/jars/*:/etc/hadoop/:/etc/hadoop:/opt/hadoop-3.3.0/share/hadoop/common/lib/*:/opt/hadoop-3.3.0/share/hadoop/common/*:/opt/hadoop-3.3.0/share/hadoop/hdfs:/opt/hadoop-3.3.0/share/hadoop/hdfs/lib/*:/opt/hadoop-3.3.0/share/hadoop/hdfs/*:/opt/hadoop-3.3.0/share/hadoop/mapreduce/*:/opt/hadoop-3.3.0/share/hadoop/yarn:/opt/hadoop-3.3.0/share/hadoop/yarn/lib/*:/opt/hadoop-3.3.0/share/hadoop/yarn/* -Xmx4g org.apache.spark.deploy.SparkSubmit --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 --master yarn --name "Spark-SQL-Thrift-Server" spark-internal
