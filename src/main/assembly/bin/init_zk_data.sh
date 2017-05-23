#!/bin/bash

MYCAT_HOME="$(dirname `readlink -f $0`)/.."
MAIN_CLASS=io.mycat.config.loader.zkprocess.xmltozk.XmltoZkMain

JAVA_CMD=""

#function log_info <msg>
#stdout: YYYY-mm-dd HH:MM:ss INFO msg
function log_info() { date +o"%F %T INFO $1" ; }
function log_error() { date +o"%F %T ERROR $1" ; }

#01. Locate java(JRE)
java_in_wrapper="`sed -nr \
	-e 's/^wrapper.java.command=(.*)[[:blank:]]*$/\1/p' \
	$MYCAT_HOME/conf/wrapper.conf`"

# test java(JRE) in this order: 
#  wrapper.conf's java -> $JAVA_HOME/bin/java -> $PATH/java
for java_cmd in "$java_in_wrapper" "$JAVA_HOME/bin/java" "java" ; do
	if $java_cmd -Xmx1m -version &>/dev/null ; then
		JAVA_CMD=$java_cmd
		break
	fi
done

if [ "$JAVA_CMD" == "" ]; then
	cat <<EOF
`date +'%F %T'` ERROR Not found usable java in following path:
$java_in_wrapper, $JAVA_HOME/bin/java, \$PATH/java
Operations would not going on.
EOF
	exit 1
fi

log_info "JAVA_CMD=$JAVA_CMD"

#02. Initialize /mycat of ZooKeeper
log_info "Start to initialize /mycat of ZooKeeper"

if ! $JAVA_CMD -Xms256M -Xmx1G  -DMYCAT_HOME=$MYCAT_HOME -cp "$MYCAT_HOME/conf:$MYCAT_HOME/lib/*" $MAIN_CLASS ; then
	log_error "Something wrong happened, please refer logs above"
	exit 1
fi

log_info "Done"
exit 0
