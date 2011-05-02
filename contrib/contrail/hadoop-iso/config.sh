#!/bin/sh

lh config \
  --distribution lenny --memtest none --binary-indices false \
  --syslinux-timeout 1 --archive-areas "main contrib non-free" 
  --iso-application "Hadoop Live" \
  --iso-publisher "Contrail Project; http://contrail-project.eu" \
  --iso-volume "Hadoop4Contrail"


# package list
cp cust/hadoop-livecd.list config/chroot_local-packageslists/

# cloudera sources and keys for apt
cp cust/cloudera.list config/chroot_sources/cloudera.chroot
cp cust/cloudera.list config/chroot_sources/cloudera.binary
cp cust/cloudera.gpg  config/chroot_sources/cloudera.chroot.gpg
cp cust/cloudera.gpg  config/chroot_sources/cloudera.binary.gpg

# we accept the terms of licence
LICENCE_FILE="config/chroot_local-preseed/sun-licence"
echo "sun-java6-bin shared/accepted-sun-dlj-v1-1 select true" >  $LICENCE_FILE
echo "sun-java6-jre shared/accepted-sun-dlj-v1-1 select true" >> $LICENCE_FILE

mkdir -p config/chroot_local-includes/etc/init.d && \
  cp cust/vmcontext config/chroot_local-includes/etc/init.d && \
  chmod 755 config/chroot_local-includes/etc/init.d/vmcontext

cp cust/post-install.sh config/chroot_local-hooks/00-post-install.sh
#test -f /usr/share/live-helper/hooks/stripped && \
#  cp /usr/share/live-helper/hooks/stripped config/chroot_local-hooks/01-stripped
