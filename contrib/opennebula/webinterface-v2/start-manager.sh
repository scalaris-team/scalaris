#!/bin/bash

# get opennebula context
mount -t iso9660 /dev/sr1 /mnt
if [ -f /mnt/context.sh ]; then
  . /mnt/context.sh
fi
umount /mnt

cd /var/lib/sc-manager
# the sleep command is used to find error messages
screen -d -m /bin/bash -c "VMID=$VMID /var/lib/sc-manager/manager.rb; sleep 365d"
cd -
