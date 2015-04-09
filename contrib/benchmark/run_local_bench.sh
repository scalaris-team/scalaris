#!/bin/bash

RUN=$1
source $RUN


cd ../../bin/

SERVER=1

START_TIME=`date +%m%d%y%H%m%S`
REV=`git log --pretty=format:'%h' -n 1`
LOG_FILE="../contrib/benchmark/log-$RUN-$REV-$START_TIME"
LOG_FILE_CLEAN="../contrib/benchmark/sum-$RUN-$REV-$START_TIME"
HOSTS="localhost"


date
for CLIENTS_PER_SERVER in $CLIENTS_PER_SERVER_LIST
do
for VMS_PER_SERVER in $VMS_PER_SERVER_LIST
do
  for CSNODES_PER_SERVER in $CSNODES_PER_SERVER_LIST
  do
        
	NODES_VM=$((CSNODES_PER_SERVER/VMS_PER_SERVER))
	CLIENTS_PER_VM=$((CLIENTS_PER_SERVER/VMS_PER_SERVER))
        ITERATIONS_PER_CLIENT=$((ITERATIONS_PER_SERVER/(CLIENTS_PER_SERVER)))
	#ITERATION=$((ITERATIONS/(VMS_PER_SERVER*SERVER)))
	if [ $NODES_VM -gt 0 ]; then
        if [ $CLIENTS_PER_VM -gt 0 ]; then
	        i=0
	        RING_SIZE=$((SERVER*VMS_PER_SERVER*NODES_VM))
	        echo "######################"
	        echo "RS $RING_SIZE VPS $VMS_PER_SERVER NPS $CSNODES_PER_SERVER NPV $NODES_VM C: $CLIENTS_PER_SERVER IT: $ITERATIONS_PER_CLIENT"
		    for host in $HOSTS
	        do
	                for vm in `seq 1 $VMS_PER_SERVER`
	                do
	                        i=$((i+1))
	                        
	                        case "$i" in
	                                1 )     BOOTIP=`/sbin/ifconfig  eth0 | grep inet\ | awk '{ print $2 }' | cut -c 6- | sed 's/\./,/g'`
	                                        echo "{boot_host, {{$BOOTIP},14195,boot}}." > scalaris.local.cfg
	                                        echo "{log_host,{{$BOOTIP},14195,boot_logger}}." >> scalaris.local.cfg
						                              echo "####################################################################################" >> $LOG_FILE
	                                        echo "SV: $SERVER RS $RING_SIZE VPS $VMS_PER_SERVER NPS $CSNODES_PER_SERVER NPV $NODES_VM C: $CLIENTS_PER_SERVER IT: $ITERATIONS_PER_CLIENT" >> $LOG_FILE
						                              ./bench_master.sh $NODES_VM $CLIENTS_PER_VM $ITERATIONS_PER_CLIENT $RING_SIZE >> $LOG_FILE &
						                              BOOTPID=$! 
						                              ;;
	                                * )     ./bench_slave.sh  $NODES_VM $i   >> log_$host-$i & ;;
	                        esac
	
	                done
	        done
	    wait $BOOTPID
		killall -9 bench_slave.sh
		killall -9 beam.smp
		sleep 1 
	fi
        fi
  done
done
done
cat $LOG_FILE | egrep 1\/s\|NPV | awk '{ if($1 == "SV:") {  b = $0;  x=1} if($1 == "1/s:") {  b = b " " $0;  x++}if($1 == "1/s:") {  b = b " " $0;  x++}if(x==3) print b}' | sort -r -n -k 16 > $LOG_FILE_CLEAN
echo "Best config for your System:"
head -n1 $LOG_FILE_CLEAN
date
