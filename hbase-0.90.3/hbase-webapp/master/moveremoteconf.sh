#!/bin/bash
#Filename: moveremoteconf.sh

server=$1
confdir=$2
hbasedir=$3
command=$4
filename=`basename $confdir`
libdir="$hbasedir/lib"
dirname=`dirname $confdir`

if [ "$command" = "get" ]
then
	#clean hbase-conf hbase lib
	rm -fr $filename
	rm -fr lib
	rm -f  hbase-*.jar
	#get hbase-conf
	scp -r $server:$confdir $filename
	scp -r $server:$libdir lib
	scp -r $server:$hbasedir/hbase-*.jar .
elif [ "$command" = "put" ]
then
	#put hbase-conf,hbase lib 
	ssh $server "cd $dirname; mv $filename $filename.back; cd $hbasedir; mv lib lib.back; rm -f hbase-*.jar"
	scp -r $filename $server:${dirname}/
	scp -r lib $server:${hbasedir}/
	scp -r hbase-*.jar $server:${hbasedir}/
	rm -fr $filename
        rm -fr lib
        rm -f  hbase-*.jar
elif [ "$command" = "distribute" ]
then
	#distribute hbase-site.xml to server
	filename="hbase-site.xml"
	scp -r $filename $server:$confdir/
	rm -f $filename
fi
