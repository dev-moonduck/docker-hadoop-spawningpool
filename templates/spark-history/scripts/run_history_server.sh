#!/bin/bash

su --preserve-environment spark -c "$SPARK_HOME/sbin/start-history-server.sh --properties-file $SPARK_HOME/conf/history_server.conf"
