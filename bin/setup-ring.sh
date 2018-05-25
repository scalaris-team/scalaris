RINGSIZE=4

SCALARIS_UNITTEST_PORT="14195"
SCALARIS_UNITTEST_YAWS_PORT="8000"

SESSIONKEY=`cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z' | fold -w 32 | head -n 1`

usage(){
    echo "usage: setup-ring [options] <cmd>"
    echo " options:"
    echo "    --ring-size <number>   - the number of nodes"
    echo " <cmd>:"
    echo "    start       - start a ring"
    echo "    stop        - stop a ring"
    exit $1
}

start_ring(){
    KEYS=`./bin/scalarisctl -t nostart -e "-noinput -eval \"io:format('\n%~p', [api_rt:escaped_list_of_keys(api_rt:get_evenly_spaced_keys($RINGSIZE))]), halt(0)\"" start | grep % | cut -c 3- | rev | cut -c 2- | rev`

    export SCALARIS_ADDITIONAL_PARAMETERS="-scalaris mgmt_server {{127,0,0,1},$SCALARIS_UNITTEST_PORT,mgmt_server} -scalaris known_hosts [{{127,0,0,1},$SCALARIS_UNITTEST_PORT,service_per_vm}] -scalaris verbatim $SESSIONKEY"
    idx=0
    for key in $KEYS; do
        let idx+=1

        STARTTYPE=null
        if [ "x$idx" == x1 ]
        then
            STARTTYPE="first -m"
            TESTPORT=$SCALARIS_UNITTEST_PORT
            YAWSPORT=$SCALARIS_UNITTEST_YAWS_PORT
        else
            STARTTYPE="joining"
            let TESTPORT=$SCALARIS_UNITTEST_PORT+$idx-1
            let YAWSPORT=$SCALARIS_UNITTEST_YAWS_PORT+$idx-1
        fi

        ./bin/scalarisctl -d -k $key -p $TESTPORT -y $YAWSPORT -t $STARTTYPE start

    done

    let TESTPORT=$SCALARIS_UNITTEST_PORT+$RINGSIZE
    let YAWSPORT=$SCALARIS_UNITTEST_YAWS_PORT+$RINGSIZE

    ./bin/scalarisctl -t nostart  -p $TESTPORT -y $YAWSPORT -e "-noinput -eval \"jsonclient:wait_for_ring_size($RINGSIZE, 10000, {127,0,0,1}, $SCALARIS_UNITTEST_YAWS_PORT), halt(0).\"" start
}

stop_ring(){
    ps aux | grep beam.smp | grep "verbatim $SESSIONKEY" | awk '{print $2}'  | xargs -n1 kill -9
}

until [ -z "$1" ]; do
    OPTIND=1
    case $1 in
        "--help")
            shift
            usage 0;;
        "--ring-size")
            shift
            RINGSIZE=$1
            shift;;
        "--session-key")
            shift
            SESSIONKEY=$1
            shift;;
        start | stop)
            cmd="$1"
            shift;;
        *)
            shift
            usage 1
    esac
done

case $cmd in
    start)
        start_ring;;
    stop)
        stop_ring;;
    *)
        echo "Unknown command: $cmd."
        usage 1;;
esac
