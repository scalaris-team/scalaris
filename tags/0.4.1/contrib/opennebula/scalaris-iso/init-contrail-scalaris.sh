#!/bin/bash

# get opennebula context
mount -t iso9660 /dev/sr1 /mnt
if [ -f /mnt/context.sh ]; then
  . /mnt/context.sh
fi
umount /mnt

ADDR=`ifconfig eth0 | grep "inet addr:" | cut -d ':' -f 2 | cut -d ' ' -f 1`
if [ "x$SCALARIS_FIRST" = xtrue ]; then
  # only eth0 for the moment
  ERLANG_ADDR=`echo $ADDR | tr . ,`

  echo "{known_hosts, [{{$ERLANG_ADDR}, 14195, service_per_vm}]}." >> /etc/scalaris/scalaris.local.cfg
  echo "{mgmt_server, {{$ERLANG_ADDR}, 14195, mgmt_server}}." >> /etc/scalaris/scalaris.local.cfg
  SCALARIS_PARAMS="-f -m"
fi

if [ "x$SCALARIS_FIRST" = xfalse ]; then
  SCALARIS_PARAMS=""

  if [ "x$SCALARIS_KNOWN_HOSTS" != "x" ]; then
    echo "$SCALARIS_KNOWN_HOSTS" >> /etc/scalaris/scalaris.local.cfg
  fi

  if [ "x$SCALARIS_MGMT_SERVER" != "x" ]; then
    echo "$SCALARIS_MGMT_SERVER" >> /etc/scalaris/scalaris.local.cfg
  fi
fi


# temporary fix, we are waiting for a real scalaris user
export HOME=/root

# the sleep command is used to find error messages
screen -d -m /bin/bash -c "/usr/bin/scalarisctl -s -n node@$ADDR -p 14195 -y 8000 $SCALARIS_PARAMS start; sleep 365d"

/var/lib/sc-manager/start-manager.sh

/etc/init.d/iptables stop
