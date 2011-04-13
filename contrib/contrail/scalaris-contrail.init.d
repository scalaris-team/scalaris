#!/bin/bash
#
# chkconfig: 2345 99 1
#
# Author:       Thorsten Schuett <schuett@zib.de>
#
# description:  starting and stopping scalaris for contrail
#

# source function library
. /etc/rc.d/init.d/functions

lockfile=/var/lock/subsys/scalaris-contrail

RETVAL=0

start() {
        echo -n $"Starting Scalaris (Contrail Edition): "

        # for production
        /etc/scalaris/init-contrail.sh

        touch "$lockfile" && success || failure
        RETVAL=$?
        echo
}

stop() {
        echo -n $"Stopping Scalaris (Contrail Edition): "

        /bin/rm "$lockfile" 2> /dev/null && success || failure
        RETVAL=$?
        echo
}

case "$1" in
  start)
        start
        ;;
  stop)
        stop
        ;;
  *)
        echo $"Usage: $0 {start|stop}"
        exit 1
esac

exit $RETVAL
