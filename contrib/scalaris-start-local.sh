#!/bin/bash


################################################################################
#  Start a Scalaris ring on a local machine with multiple VMs and multiple dht
#  nodes per VM. Details see print_usage().
#
#  For frequent add somethin like the following to your shell configuration:
#  function starts() {
#      $SCALARIS_DIR/contrib/scalaris_start_local -asw $1 $2
#  }
#  alias stops="$SCALARIS_DIR/contrib/scalaris_start_local.sh stop"
################################################################################

SCALARIS_DIR=${SCALARIS_DIR:-$HOME/scalaris}
ETCDIR=$SCALARIS_DIR/bin
BINDIR=$SCALARIS_DIR/bin
BEAMDIR=$SCALARIS_DIR/ebin
SHUFFLE_NODE_IDS=0
ATTACH=${ATTACH:-false}
WAIT_FOR_STABLE_RING=${WAIT_FOR_STABLE_RING=-false}
DHT_NODES_PER_VM=${DHT_NODES_PER_VM:-1}

function start_servers() {
    echo "starting Scalaris with $VMS_PER_NODE VMs and $DHT_NODES_PER_VM dht nodes per vm..."

    let NR_OF_DHT_NODES=$VMS_PER_NODE\*$DHT_NODES_PER_VM
    let NR_OF_VMS=$VMS_PER_NODE

    KEYLIST=""
    if [ $SHUFFLE_NODE_IDS -eq 1 ]
    then
        KEYLIST=`erl -name bench_ -pa $BEAMDIR -noinput -eval "L = util:lists_split(util:shuffle(api_dht_raw:split_ring($NR_OF_DHT_NODES)), $NR_OF_VMS), io:format('~p', [L]), halt(0)."`
    else
        KEYLIST=`erl -name bench_ -pa $BEAMDIR -noinput -eval "L = util:lists_split(api_dht_raw:split_ring($NR_OF_DHT_NODES), $NR_OF_VMS), io:format('~p', [L]), halt(0)."`
    fi

    PORT=14195
    YAWSPORT=8000

    VM_IDX=1
    JOIN_KEYS=`erl -name bench_ -noinput -eval "L = lists:nth($VM_IDX, $KEYLIST), io:format('~p', [L]), halt(0)."`
    $BINDIR/scalarisctl -j "$JOIN_KEYS" -n first -p $PORT -y $YAWSPORT --nodes-per-vm $DHT_NODES_PER_VM --screen -d -m -t first start
    let PORT+=1
    let YAWSPORT+=1
    let VM_IDX+=1
    for NODE in $(seq 2 $VMS_PER_NODE); do
        JOIN_KEYS=`erl -name bench_ -noinput -eval "L = lists:nth($VM_IDX, $KEYLIST), io:format('~p', [L]), halt(0)."`
        $BINDIR/scalarisctl -j "$JOIN_KEYS" -n node$PORT -p $PORT -y $YAWSPORT --nodes-per-vm $DHT_NODES_PER_VM --screen -d -t joining start
        let PORT+=1
        let YAWSPORT+=1
        let VM_IDX+=1
    done
}

function wait_for_stable_ring {
    echo "waiting for stable ring..."
    let NR_OF_NODES=$VMS_PER_NODE\*$DHT_NODES_PER_VM

    # wait for the first VM to start
    NR_OF_FIRSTS=`epmd -names | grep 'name first at port' | wc -l`
    while [ $NR_OF_FIRSTS -ne 1 ]
    do
        NR_OF_FIRSTS=`epmd -names | grep 'name first at port' | wc -l`
    done
    # wait for the first VM to initialize
    erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "A = rpc:call('first@`hostname -f`', api_vm, wait_for_scalaris_to_start, []), io:format('\tserver started: ~p~n', [A]), halt(0)."
    # wait for the ring to stabilize
    erl -setcookie "chocolate chip cookie" -name bench_ -noinput -eval "A = rpc:call('first@`hostname -f`', admin, wait_for_stable_ring, [$NR_OF_NODES]), io:format('\tring stable: ~p~n', [A]), halt(0)."
}

function kill_old_nodes() {
    NO_OF_SESSIONS=$(screen -ls | grep Detached | grep scalaris_ | wc -l)
    if [[ $NO_OF_SESSIONS -gt 0 ]]; then
        echo "killing $NO_OF_SESSIONS VMs"
        screen -ls | grep Detached | grep scalaris_ | cut -d. -f1 | awk '{print $1}' | xargs -r kill
    fi
}

print_usage() {
cat << EOF

Synopsis
  $(basename $0) [-w] [-h] [stop | <no of vms> [ <no_of_dht_nodes> ]
  Start a Scalaris ring on a local machine with multiple WMs and multiple DHT
  nodes per VM.
  The nodes are placed evenly across the ring. Still running ring will be killed
  automatically.

 -a
    attach to the screen session of the first node

 -s
    shuffle ids

 -w
    Wait for a stable ring.

 -h
    Print this help.

  -v
    Enabble verbose mode, i.e. bash debugging ('set -x').
EOF
}

############
#   MAIN   #
############

# show usage if invoked without options/arguments
if [ $# -eq 0 ]; then
  print_usage
  exit 1
fi


# parse options
while getopts :a:hsvw opt; do
  case $opt in
    a)
        ATTACH=true
        ATTACH_NODE=$OPTARG
        ;;
    s)
        SHUFFLE_NODE_IDS=1
        ;;
    w)
        WAIT_FOR_STABLE_RING=true
        ;;
    h)
        print_usage
        exit 1
        ;;
    v)
      set -x
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

# shift the arguments parsed by getopts away
shift $((OPTIND-1))

# get the mass arguments (either stop or <no of vms> <no of dht nodes per vm>
if [[ -n $1 ]]; then
    if [[ $1 = "stop"  ]]; then
        kill_old_nodes
        exit 0
    else
        VMS_PER_NODE=$1
    fi
fi
if [[ -n $2 ]]; then
    DHT_NODES_PER_VM=$2
fi

kill_old_nodes
start_servers

if $WAIT_FOR_STABLE_RING; then
    wait_for_stable_ring
fi

# attach to node given be -a
if $ATTACH; then
    screen -r $(screen -ls | grep scalaris_$ATTACH_NODE | cut -d. -f1 | awk '{print $1}')
fi

