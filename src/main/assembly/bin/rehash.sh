#!/bin/sh

#check JAVA_HOME & java
noJavaHome=false
if [ -z "$JAVA_HOME" ] ; then
    noJavaHome=true
fi
if [ ! -e "$JAVA_HOME/bin/java" ] ; then
    noJavaHome=true
fi
if $noJavaHome ; then
    echo
    echo "Error: JAVA_HOME environment variable is not set."
    echo
    exit 1
fi
#==============================================================================
#set JAVA_OPTS
JAVA_OPTS="-server -Xms2G -Xmx2G -XX:MaxPermSize=64M  -XX:+AggressiveOpts -XX:MaxDirectMemorySize=2G"
#JAVA_OPTS="-server -Xms4G -Xmx4G -XX:MaxPermSize=64M  -XX:+AggressiveOpts -XX:MaxDirectMemorySize=6G"
#performance Options
#JAVA_OPTS="$JAVA_OPTS -Xss256k"
#JAVA_OPTS="$JAVA_OPTS -XX:+AggressiveOpts"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseBiasedLocking"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseFastAccessorMethods"
#JAVA_OPTS="$JAVA_OPTS -XX:+DisableExplicitGC"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"
#JAVA_OPTS="$JAVA_OPTS -XX:+CMSParallelRemarkEnabled"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSCompactAtFullCollection"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly"
#JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=75"
#JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=75"
#GC Log Options
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCApplicationStoppedTime"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCTimeStamps"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails"
#debug Options
#JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,address=8065,server=y,suspend=n"
#==============================================================================

#set HOME
CURR_DIR=`pwd`
cd `dirname "$0"`/..
MYCAT_HOME=`pwd`
cd $CURR_DIR
if [ -z "$MYCAT_HOME" ] ; then
    echo
    echo "Error: MYCAT_HOME environment variable is not defined correctly."
    echo
    exit 1
fi
#==============================================================================

#set CLASSPATH
MYCAT_CLASSPATH="$MYCAT_HOME/conf:$MYCAT_HOME/lib/classes"
for i in "$MYCAT_HOME"/lib/*.jar
do
    MYCAT_CLASSPATH="$MYCAT_CLASSPATH:$i"
done
#==============================================================================

#startup Server
RUN_CMD="\"$JAVA_HOME/bin/java\""
RUN_CMD="$RUN_CMD -DMYCAT_HOME=\"$MYCAT_HOME\""
RUN_CMD="$RUN_CMD -classpath \"$MYCAT_CLASSPATH\""
RUN_CMD="$RUN_CMD $JAVA_OPTS"
RUN_CMD="$RUN_CMD io.mycat.util.rehasher.RehashLauncher "
#to specify the following main args
#RUN_CMD="$RUN_CMD -jdbcDriver="
#RUN_CMD="$RUN_CMD -jdbcUrl="
#RUN_CMD="$RUN_CMD -host="
#RUN_CMD="$RUN_CMD -user="
#RUN_CMD="$RUN_CMD -database="
#RUN_CMD="$RUN_CMD -password="
#RUN_CMD="$RUN_CMD -tablesFile="
#RUN_CMD="$RUN_CMD -shardingField="
#RUN_CMD="$RUN_CMD -rehashHostsFile="
#RUN_CMD="$RUN_CMD -hashType="
#RUN_CMD="$RUN_CMD -seed="
#RUN_CMD="$RUN_CMD -virtualBucketTimes="
#RUN_CMD="$RUN_CMD -weightMapFile="
#RUN_CMD="$RUN_CMD -rehashNodeDir="

echo $RUN_CMD
eval $RUN_CMD
#==============================================================================
