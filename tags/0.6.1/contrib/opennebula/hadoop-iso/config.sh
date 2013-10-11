#!/bin/bash

#  -m http://cdn.debian.de/debian/ \
#  --bootloader syslinux --syslinux-menu false \



lh config \
  --syslinux-timeout 1 --binary-indices false \
  --archive-areas "main contrib non-free" \
  --distribution stable --memtest none \
  --iso-application "Hadoop Live" \
  --iso-publisher "Contrail Project; http://contrail-project.eu" \
  --cache-indices true \
  --iso-volume "Hadoop4Contrail"


# package list
cp cust/hadoop-livecd.list config/chroot_local-packageslists/

# cloudera sources and keys for apt
cp cust/cloudera.list config/chroot_sources/cloudera.chroot
cp cust/cloudera.list config/chroot_sources/cloudera.binary
cp cust/cloudera.gpg  config/chroot_sources/cloudera.chroot.gpg
cp cust/cloudera.gpg  config/chroot_sources/cloudera.binary.gpg
cp cust/xtreemfs.list config/chroot_sources/xtreemfs.chroot
cp cust/xtreemfs.list config/chroot_sources/xtreemfs.binary
cp cust/xtreemfs.gpg  config/chroot_sources/xtreemfs.chroot.gpg
cp cust/xtreemfs.gpg  config/chroot_sources/xtreemfs.binary.gpg

# xtreemfs client
#test -f config/chroot_local-packages/xtreemfs-client_1.3.0.83_amd64.deb ||
#  wget --directory-prefix=config/chroot_local-packages \
#  http://download.opensuse.org/repositories/home:/xtreemfs:/unstable/Debian_6.0/amd64/xtreemfs-client_1.3.1.82_amd64.deb

# we accept the terms of licence
#LICENCE_FILE="config/chroot_local-preseed/sun-licence"
#echo "sun-java6-bin shared/accepted-sun-dlj-v1-1 select true" >  $LICENCE_FILE
#echo "sun-java6-jre shared/accepted-sun-dlj-v1-1 select true" >> $LICENCE_FILE

mkdir -p config/chroot_local-includes/root/conf.cluster
cp cust/conf.cluster/* config/chroot_local-includes/root/conf.cluster

install -D -m 755 cust/vmcontext config/chroot_local-includes/etc/init.d/vmcontext
install -D -m 755 cust/start-hadoop.sh config/chroot_local-includes/etc/init.d/hadoop 
install -D -m 755 cust/dnsupdate-client.pl config/chroot_local-includes/usr/bin/dnsupdate-client.pl
install -D -m 755 cust/dnsupdate-server.pl config/chroot_local-includes/usr/bin/dnsupdate-server.pl
install -D -m 644 cust/analyze_logs.pig config/chroot_local-includes/root/analyze_logs.pig
install -D -m 755 cust/analyze_logs.pl config/chroot_local-includes/root/analyze_logs.pl
install -D -m 644 cust/bydate.gnuplot config/chroot_local-includes/root/bydate.gnuplot
install -D -m 644 cust/gmond.conf config/chroot_local-includes/etc/ganglia/gmond.conf

mkdir -p config/chroot_local-includes/usr/lib/scalaris/contrib
rm -rf config/chroot_local-includes/usr/lib/scalaris/contrib/opennebula
svn up ../webinterface-v2
cp -R ../webinterface-v2 config/chroot_local-includes/usr/lib/scalaris/contrib/opennebula
 
install -D -m 755 cust/one.rb config/chroot_local-includes/usr/lib/scalaris/contrib/opennebula/one.rb

cp cust/post-install.sh config/chroot_local-hooks/00-post-install.sh
#test -f /usr/share/live-helper/hooks/stripped && \
#  cp /usr/share/live-helper/hooks/stripped config/chroot_local-hooks/01-stripped

# get and compile pig(gybank)
if [ ! -d cust/pig ]; then
  mkdir -p config/chroot_local-includes/root
  svn checkout http://svn.apache.org/repos/asf/pig/trunk/ cust/pig
#else
#  svn update cust/pig
fi

if [ ! -f cust/pig/contrib/piggybank/java/piggybank.jar ]; then
  cp -u cust/ContrailLogLoader.java cust/pig/contrib/piggybank/java/src/main/java/org/apache/pig/piggybank/storage/apachelog/
  pushd cust/pig && ant && cd contrib/piggybank/java && ant && popd
fi
cp -u cust/pig/contrib/piggybank/java/piggybank.jar config/chroot_local-includes/root
