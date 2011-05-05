LIVEFLAGS=--cache=/var/cache/live

all: test

isos: Scalaris-LiveCD.iso scalaris-context.iso

Scalaris-LiveCD.iso: scalaris.ks init-contrail.sh scalaris-contrail.init.d vmcontext
	sudo livecd-creator $(LIVEFLAGS) --config=scalaris.ks --fslabel=Scalaris-LiveCD

scalaris-fc15-livecd.iso: scalaris-fc15.ks init-contrail.sh scalaris-contrail.init.d vmcontext
	sudo livecd-creator $(LIVEFLAGS) --config=scalaris-fc15.ks --fslabel=scalaris-fc15-livecd

scalaris-context.iso: context.sh
	mkisofs -U -J -R -f -o scalaris-context.iso context.sh

test: Scalaris-LiveCD.iso scalaris-context.iso
	qemu-kvm -m 512 -drive file=Scalaris-LiveCD.iso,index=0,media=cdrom -drive file=scalaris-context.iso,index=1,media=cdrom

test-fc15: scalaris-fc15-livecd.iso scalaris-context.iso
	qemu-kvm -m 512 -drive file=scalaris-fc15-livecd.iso,index=0,media=cdrom -drive file=scalaris-context.iso,index=1,media=cdrom

