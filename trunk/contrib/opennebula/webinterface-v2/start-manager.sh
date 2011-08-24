#!/bin/bash

# get opennebula context
if [ -f /root/context.sh ]; then
  . /root/context.sh
else
  mount -t iso9660 /dev/sr1 /mnt
  if [ -f /mnt/context.sh ]; then
    . /mnt/context.sh
  fi
  umount /mnt
fi

if [ -z  "$VMID" ]; then
  echo "oops. could not load context.sh"
  exit 1
fi

mkdir -p /var/lib/sc-manager/public
cd /var/lib/sc-manager
# the sleep command is used to find error messages
screen -d -m /bin/bash -c "VMID=$VMID /var/lib/sc-manager/manager.rb; sleep 365d"
cd -
