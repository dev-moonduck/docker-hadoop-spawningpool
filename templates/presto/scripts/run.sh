#!/bin/bash

echo "node.environment=production" >> $PRESTO_HOME/etc/node.properties
echo "node.id=$PRESTO_NODE_ID" >> $PRESTO_HOME/etc/node.properties
echo "node.data-dir=/var/presto/data" >> $PRESTO_HOME/etc/node.properties

$PRESTO_HOME/bin/launcher start
