#!/bin/bash

echo "check JAVA_HOME & java"
JAVA_CMD=$JAVA_HOME/bin/java
MAIN_CLASS=org.opencloudb.performance.TestInsertPerf
if [ ! -d "$JAVA_HOME" ]; then
    echo ---------------------------------------------------
    echo WARN: JAVA_HOME environment variable is not set. 
    echo ---------------------------------------------------
    JAVA_CMD=java
fi

echo "---------set HOME_DIR------------"
CURR_DIR=`pwd`
cd ..
MYCAT_HOME=`pwd`
cd $CURR_DIR
$JAVA_CMD -Xms256M -Xmx1G -XX:MaxPermSize=64M  -DMYCAT_HOME=$MYCAT_HOME -cp "$MYCAT_HOME/conf:$MYCAT_HOME/lib/*" $MAIN_CLASS $1 $2 $3 $4 $5
