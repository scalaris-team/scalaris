LIVEFLAGS=--cache=/var/cache/live --compression-type=gzip

all: test

isos: Scalaris-LiveCD.iso Scalaris-Wiki-LiveCD.iso scalaris-context.iso

Scalaris-LiveCD.iso: scalaris.ks init-contrail-scalaris.sh scalaris-contrail.init.d vmcontext
	sudo livecd-creator $(LIVEFLAGS) --config=scalaris.ks --fslabel=Scalaris-LiveCD

Scalaris-Wiki-LiveCD.iso: scalaris-wiki.ks init-contrail-wiki.sh scalaris-contrail.init.d vmcontext
	sudo livecd-creator $(LIVEFLAGS) --config=scalaris-wiki.ks --fslabel=Scalaris-Wiki-LiveCD

scalaris-context.iso: context.sh
	mkisofs -U -J -R -f -o scalaris-context.iso context.sh

test: Scalaris-LiveCD.iso scalaris-context.iso
	qemu-kvm -m 512 -drive file=Scalaris-LiveCD.iso,index=0,media=cdrom -drive file=scalaris-context.iso,index=1,media=cdrom

test-wiki: Scalaris-Wiki-LiveCD.iso scalaris-context.iso
	qemu-kvm -m 512 -drive file=Scalaris-Wiki-LiveCD.iso,index=0,media=cdrom -drive file=scalaris-context.iso,index=1,media=cdrom

