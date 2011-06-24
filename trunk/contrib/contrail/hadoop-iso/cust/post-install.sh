#!/bin/sh
# script to be run after installation process in chroot environment
#update-rc.d -f avahi-daemon remove
update-rc.d vmcontext defaults
#update-rc.d hadoop defaults
update-rc.d -f hadoop-0.20-namenode remove
update-rc.d -f hadoop-0.20-datanode remove
update-rc.d -f hadoop-0.20-secondarynamenode remove
update-rc.d -f hadoop-0.20-jobtracker remove
update-rc.d -f hadoop-0.20-tasktracker remove
update-rc.d -f hue remove
update-rc.d -f dnsmasq remove

ln -s /etc/init.d/hadoop /etc/rc2.d/S99hadoop

apt-get remove --purge -y dhcp3-client dhcp3-common

echo "LC_ALL=C" > /etc/default/locale
echo "Europe/Berlin" > /etc/timezone

cat << EOT >> /etc/hadoop/conf/hadoop-env.sh 
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true -Djava.security.egd=file:/dev/./urandom"
EOT

rm -f /etc/hadoop/conf/masters /etc/hadoop/conf/slaves

cat << EOT > /etc/dnsmasq.conf
expand-hosts
domain=localcloud
local=/localcloud/
EOT

