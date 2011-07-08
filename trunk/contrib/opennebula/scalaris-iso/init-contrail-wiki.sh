#!/bin/bash

# get opennebula context
mount -t iso9660 /dev/sr1 /mnt
if [ -f /mnt/context.sh ]; then
  . /mnt/context.sh
fi
umount /mnt

if [ "x$SCALARIS_NODE" != "x" ]; then
    echo "scalaris.node=$SCALARIS_NODE" > /usr/share/tomcat6/webapps/scalaris-wiki/WEB-INF/scalaris.properties
fi

/etc/init.d/tomcat6 start