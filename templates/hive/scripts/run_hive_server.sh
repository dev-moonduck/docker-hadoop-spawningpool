#!/bin/bash

su --preserve-environment hive -c "cd $HIVE_HOME/bin && ./hiveserver2"
