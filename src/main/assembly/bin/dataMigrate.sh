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
RUN_CMD="$RUN_CMD io.mycat.util.dataMigrator.DataMigrator "
#to specify the following main args
#临时文件路径,目录不存在将自动创建，不指定此目录则默认为mycat根下的temp目录
RUN_CMD="$RUN_CMD -tempFileDir="
#默认true：不论是否发生主备切换，都使用主数据源数据，false：使用当前数据源                  
RUN_CMD="$RUN_CMD -isAwaysUseMaster=true"
#mysql bin路径
RUN_CMD="$RUN_CMD -mysqlBin="
#mysqldump命令行长度限制字节数 默认110k
RUN_CMD="$RUN_CMD -cmdLength=110*1024"
#导入导出数据所用字符集 默认utf8
RUN_CMD="$RUN_CMD -charset=utf8"
#完成扩容缩容后是否删除临时文件 默认为true
RUN_CMD="$RUN_CMD -deleteTempFileDir=true"
#performance Options
#并行线程数（涉及生成中间文件和导入导出数据）默认为迁移程序所在主机环境的cpu核数*2
RUN_CMD="$RUN_CMD -threadCount="
#每个数据库主机上清理冗余数据的并发线程数，默认为当前脚本程序所在主机cpu核数/2
RUN_CMD="$RUN_CMD -delThreadCount="
#读取迁移节点全部数据时一次加载的数据量 默认100000                    
RUN_CMD="$RUN_CMD -queryPageSize="

echo $RUN_CMD
eval $RUN_CMD
#==============================================================================
