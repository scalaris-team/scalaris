lang en_US.UTF-8
keyboard de-latin1-nodeadkeys
timezone US/Eastern
auth --useshadow --enablemd5
selinux --enforcing
firewall --disabled
part / --size 2048
firstboot --disable
bootloader --timeout=1 --append="3"

repo --name=fedora-15 --mirrorlist=http://mirrors.fedoraproject.org/mirrorlist?repo=fedora-15&arch=$basearch
repo --name=scalaris --baseurl=http://download.opensuse.org/repositories/home:/scalaris/Fedora_15/

%packages
@core
anaconda-runtime
bash
kernel
passwd
policycoreutils
chkconfig
authconfig
rootfiles

# for scalaris
scalaris-svn
ruby-scalaris-svn
screen

# for manager interface
scalaris-svn-one-manager
scalaris-svn-one-client
ruby
rubygem-oca
rubygem-sequel
rubygem-sqlite3
rubygem-json
rubygem-sinatra
rubygem-nokogiri

# for debugging
strace
less
virt-what
emacs

# minimization
anaconda
-firstboot
-system-config-date
-system-config-keyboard
-system-config-users

%post --nochroot

cp scalaris-contrail.init.d $INSTALL_ROOT/etc/init.d/scalaris-contrail
cp vmcontext $INSTALL_ROOT/etc/init.d/vmcontext
cp init-contrail.sh $INSTALL_ROOT/etc/scalaris/init-contrail.sh
chmod ugo+x $INSTALL_ROOT/etc/init.d/vmcontext
chmod ugo+x $INSTALL_ROOT/etc/scalaris/init-contrail.sh

# install webinterface
mkdir -p $INSTALL_ROOT/var/lib/sc-manager
cp -r ../webinterface-v2/* $INSTALL_ROOT/var/lib/sc-manager
rm $INSTALL_ROOT/var/lib/sc-manager/opennebula.db

# prepare opennebula-ruby binding
wget -nc --no-check-certificate https://rubygems.org/downloads/oca-1.1.2.gem
cp oca-1.1.2.gem $INSTALL_ROOT/var/lib/sc-manager/oca-1.1.2.gem
%post

# We made firstboot a native systemd service, so it can no longer be turned
# off with chkconfig. It should be possible to turn it off with systemctl, but
# that doesn't work right either. For now, this is good enough: the firstboot
# service will start up, but this tells it not to run firstboot. I suspect the
# other services 'disabled' below are not actually getting disabled properly,
# with systemd, but we can look into that later. - AdamW 2010/08 F14Alpha
echo "RUN_FIRSTBOOT=NO" > /etc/sysconfig/firstboot

# add scalaris-contrail
/sbin/chkconfig --add scalaris-contrail
/sbin/chkconfig --add vmcontext

systemctl disable NetworkManager.service

/sbin/chkconfig iptables off
/sbin/chkconfig ip6tables off

(cd /var/lib/sc-manager ; gem install --local oca-1.1.2)

%end
