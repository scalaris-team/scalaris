#!/bin/bash
# script to be run after installation process in chroot environment
#update-rc.d -f avahi-daemon remove
update-rc.d vmcontext defaults
update-rc.d hadoop defaults
update-rc.d -f hadoop-0.20-namenode remove
update-rc.d -f hadoop-0.20-datanode remove
update-rc.d -f hadoop-0.20-secondarynamenode remove
update-rc.d -f hadoop-0.20-jobtracker remove
update-rc.d -f hadoop-0.20-tasktracker remove
update-rc.d -f hue remove
update-rc.d -f dnsmasq remove
update-rc.d -f gmetad remove
update-rc.d -f ganglia-monitor remove
update-rc.d -f apache2 remove

apt-get remove --purge -y dhcp3-client dhcp3-common isc-dhcp-client isc-dhcp-common

echo "LC_ALL=C" > /etc/default/locale
echo "Europe/Berlin" > /etc/timezone

HADOOP_ETC="/etc/alternatives/hadoop-etc"
cp -a /root/conf.cluster $HADOOP_ETC/
update-alternatives --install $HADOOP_ETC/conf hadoop-0.20-conf $HADOOP_ETC/conf.cluster 99

cat << EOT > /etc/dnsmasq.conf
expand-hosts
domain=localcloud
local=/localcloud/
EOT

HADOOP_CONFIG="/etc/hadoop-0.20/conf.cluster"
test -d $HADOOP_CONFIG || (echo "Error: $HADOOP_CONFIG not found"; exit 1)
mv $HADOOP_CONFIG/hue.ini /etc/hue
for FILE in $HADOOP_CONFIG/*; do 
  FBASE=`basename $FILE`
  rm -f /etc/hadoop-0.20/conf.pseudo/$FBASE
  ln -s $FILE /etc/hadoop-0.20/conf.pseudo/$FBASE
done

for FILE in oca-1.1.2; do
  wget -nc --no-check-certificate https://rubygems.org/downloads/$FILE.gem
  mv $FILE.gem /var/lib/sc-manager/$FILE.gem
  (cd /var/lib/sc-manager ; gem install --local $FILE)
done 

# debian live user wants to be on UID 1000
groupmod -g 1001 xtreemfs
usermod -u 1001 xtreemfs
chown -R xtreemfs /var/lib/xtreemfs

cat << EOT > /root/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-6-sun/jre
export PATH=$PATH:/root/pig/bin
EOT
