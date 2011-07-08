#!/bin/bash
### BEGIN INIT INFO
# Provides:          hadoop 
# Required-Start:    vmcontext
# Required-Stop:      
# Default-Start:     2
# Default-Stop:      0 1 6
# Short-Description: starts hadoop.
# Description:       see short.
### END INIT INFO

test -f /root/context.sh || (echo "context.sh is missing"; exit 1)
source /root/context.sh

echo "nameserver $HADOOP_MASTER" > /etc/resolv.conf
if [ "x$HADOOP_FIRST" == "xtrue" ]; then
  /etc/init.d/dnsmasq start
  /usr/bin/dnsupdate-server.pl &
else
  RETVAL=1
  while [ "$RETVAL" -ne "0" ]; do
    HOST=`echo $PUBLIC_IP | sed 's/\./-/g'`
    /usr/bin/dnsupdate-client.pl $HADOOP_MASTER $IP_PUBLIC
    RETVAL=$?
    if [ "$RETVAL" -ne "0" ]; then
      sleep 1
    fi
  done
fi

HADOOP_DIR=/etc/alternatives/hadoop-etc/conf
sed -i "s/@HDFS_MASTER@/$HADOOP_MASTER/g" $HADOOP_DIR/core-site.xml
sed -i "s/@MAPRED_MASTER@/$HADOOP_MASTER/g" $HADOOP_DIR/mapred-site.xml
sed -i "s/@MAPRED_MASTER@/$HADOOP_MASTER/g" /etc/hue/hue.ini
sed -i "s/@HDFS_MASTER@/$HADOOP_MASTER/g" /etc/hue/hue.ini
#sed -i "s/hdfs:\/\/localhost:8020/file:\/\/\/mnt\/xtreemfs/" /etc/hadoop/conf/core-site.xml


NR_CPUS=`grep processor /proc/cpuinfo | wc -l`
let NR_TASKS=2*$NR_CPUS
sed -i "s/@NR_MAPTASKS@/$NR_TASKS/" $HADOOP_DIR/mapred-site.xml
sed -i "s/@NR_REDTASKS@/$NR_TASKS/" $HADOOP_DIR/mapred-site.xml


# HDFS stuff
if [ "x$HADOOP_FIRST" == "xtrue" ]; then
  /etc/init.d/hadoop-0.20-namenode start
  /etc/init.d/hadoop-0.20-secondarynamenode start
fi
/etc/init.d/hadoop-0.20-datanode start

# XtreemFS stuff
#mkdir /mnt/xtreemfs
#screen -d -m mount.xtreemfs -o allow_other cumulus/hadoop_fs /mnt/xtreemfs
#sleep 3
#mount | grep -q /mnt/xtreemfs
#if [ "$?" -ne "0" ]; then
#  echo "ERROR: XtreemFS did not show up."
#  exit 1
#fi


# MapReduce stuff
if [ "x$HADOOP_FIRST" == "xtrue" ]; then
  /etc/init.d/hadoop-0.20-jobtracker start
  /etc/init.d/hue start
fi
/etc/init.d/hadoop-0.20-tasktracker start

exit 0
