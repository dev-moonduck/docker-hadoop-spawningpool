#!/bin/bash

su --preserve-environment spark -c "$SPARK_HOME/sbin/start-history-server.sh"
