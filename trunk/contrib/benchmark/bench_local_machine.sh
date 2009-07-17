#!/bin/bash

cd ../../bin/

SERVER=1
ITERATIONS_PER_SERVER=5000

START_TIME=`date +%m%d%y%H%m%S`
LOG_FILE="../contrib/benchmark/bench_log_$START_TIME"
LOG_FILE_CLEAN="../contrib/benchmark/sum_$START_TIME"
HOSTS="localhost"
VMS_PER_SERVER_LIST="1 2"
CLIENTS_PER_SERVER_LIST="1 2 4 8 16 32"
CSNODES_PER_SERVER_LIST="1 2 4 8 16 32"
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
	                                        echo "RS $RING_SIZE VPS $VMS_PER_SERVER NPS $CSNODES_PER_SERVER NPV $NODES_VM C: $CLIENTS_PER_SERVER IT: $ITERATIONS_PER_CLIENT" >> $LOG_FILE
						                              ./bench_master.sh $NODES_VM $CLIENTS_PER_VM $ITERATIONS_PER_CLIENT $RING_SIZE >> $LOG_FILE &
						                              BOOTPID=$! 
						                              sleep 1;;
	                                * )     ./bench_slave.sh  $NODES_VM $i  & ;;
	                        esac
	
	                done
	        done
	    wait $BOOTPID
		killall -9 bench_slave.sh
		killall -9 beam.smp
		sleep 1 
	fi
  done
done
done
cat $LOG_FILE | egrep 1\/s\|NPV | xargs -n 16 | sort -r -n -k 14 > $LOG_FILE_CLEAN
echo "Best config for your System:"
head -n1 $LOG_FILE_CLEAN
date
