DIRNAME=`dirname $0`
GLOBAL_CFG="$DIRNAME/scalaris.cfg.sh"
LOCAL_CFG="$DIRNAME/scalaris.local.cfg.sh"
if [ -f "$GLOBAL_CFG" ] ; then source "$GLOBAL_CFG" ; fi
if [ -f "$LOCAL_CFG" ] ; then source "$LOCAL_CFG" ; fi

export ERL_MAX_PORTS=16384

erl $ERL_OPTS +S 1 +A 1 -setcookie "chocolate chip cookie" -pa ../contrib/log4erl/ebin -pa ../contrib/yaws/ebin -pa ../ebin -yaws embedded true -connect_all false -name boot -s simulation;
