#!/bin/bash
#Filename: restartserver.sh

server=$1
hbasedir=$2
command=$3

if [ "$command" = "start" ]
then	
	START="$hbasedir/bin/hbase-daemon.sh start regionserver"
	ssh $server $START
elif [ "$command" = "stop" ]
then
	STOP="$hbasedir/bin/hbase-daemon.sh stop regionserver"
	ssh $server $STOP
fi
