#!/bin/bash

cd $(dirname $0)

mkdir -p target
cd target 

if [ ! -f scalaris-0.6.0.tar.gz ]
then
	wget http://scalaris.googlecode.com/files/scalaris-0.6.0.tar.gz
fi

if [ ! -d  scalaris-0.6.0 ]
then
	tar xvfz  scalaris-0.6.0.tar.gz
fi

cd scalaris-0.6.0

if [ ! -f bin/scalarisctl ]
then
	sudo apt-get install erlang make
	./configure
	make
fi	

./bin/scalarisctl -m -s -f start
