
default: binary.iso 

test: binary.iso
	kvm -cdrom binary.iso -m 1024

binary.iso: Makefile config.sh cust
	make clean
	make config
	make build

clean:
	sudo lh clean

shiny: clean
	sudo rm -rf auto config cache .stage

config: config.sh
	./config.sh

build:
	sudo lh build
