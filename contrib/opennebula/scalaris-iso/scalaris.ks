lang en_US.UTF-8
keyboard de-latin1-nodeadkeys
timezone US/Eastern
auth --useshadow --enablemd5
selinux --enforcing
firewall --disabled
part / --size 1024
firstboot --disable
bootloader --timeout=1

repo --name=fedora-14 --mirrorlist=http://mirrors.fedoraproject.org/mirrorlist?repo=fedora-14&arch=$basearch
repo --name=scalaris --baseurl=http://download.opensuse.org/repositories/home:/scalaris/Fedora_14/

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
ruby
screen
# virt-what starting with FC15

# for debugging
strace

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
/sbin/chkconfig --del NetworkManager

%end
