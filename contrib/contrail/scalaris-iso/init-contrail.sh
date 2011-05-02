#!/bin/bash

# get opennebula context
mount -t iso9660 /dev/sr1 /mnt
if [ -f /mnt/context.sh ]; then
  . /mnt/context.sh
fi
umount /mnt

if [ "x$SCALARIS_FIRST" = xtrue ]; then
  # only eth0 for the moment
  ADDR=`ifconfig eth0 | grep "inet addr:" | cut -d ':' -f 2 | cut -d ' ' -f 1`
  ERLANG_ADDR=`echo $ADDR | tr . ,`

  echo "{known_host, [{{$ERLANG_ADDR}, 14195, cyclon_thread}]}." >> /etc/scalaris/scalaris.local.cfg
  SCALARIS_PARAMS="-f -m"
fi

if [ "x$SCALARIS_FIRST" = xfalse ]; then
  SCALARIS_PARAMS=""
fi

# temporary fix, we are waiting for a real scalaris user
export HOME=/root

# the sleep command is used to find error messages
screen -d -m /bin/bash -c "/usr/bin/scalarisctl -s -p 14195 -y 8000 $SCALARIS_PARAMS start; sleep 365d"

/etc/init.d/iptables stop