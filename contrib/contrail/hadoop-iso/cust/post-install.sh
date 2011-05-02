#!/bin/sh
# script to be run after installation process in chroot environment
#update-rc.d -f avahi-daemon remove
update-rc.d vmcontext defaults

# fix hadoop symlinks to get hadoop-config script running
for i in /etc/init.d/hadoop*; do
  rm "/etc/rc2.d/S20"`basename $i`
  cp -s $i "/etc/rc2.d/S20"`basename $i`
done

apt-get remove --purge -y dhcp3-client dhcp3-common

echo "LC_ALL=C" > /etc/environment
echo "Europe/Berlin" > /etc/timezone
