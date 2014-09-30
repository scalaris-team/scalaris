#!/bin/bash

# Script to create a graph (using graphviz / dot) from all cyclon caches from one cycle.
# Use the ?PRINT_CACHE_FOR_DOT macro in gossip_cyclon to print the caches to the logfile
# Only works within one VM. You may need to delete old logfiles first.

# To compare the graphs from different cycles run something like
#   for i in {0..10}; do ./dotgraph.sh $i cyclon_graph$i; done
# to get graphs for the first 10 cycles.

# set -x

CYCLE=$1
LOGFILE="scalaris_log4erl.txt"
FILENAME="cyclon"
KEEP_GV_FILE=true
RADIUS=3 # value depens on the number of nodes. Default: 3, Increase for > 10 Nodes.

if [ -n "$2" ]; then
    FILENAME=$2
else
    FILENAME="cyclon"
fi

rm -f $FILENAME.gv

cat <<EOF > $FILENAME.gv
digraph $FILENAME {
    mindist=1
    layout=neato
    edge [arrowsize=.5, arrowhead=nonenoneonormal]
    node [pin=true]
    //splines=polyline

EOF

# Get all the pids as array
Nodes=$(grep "\[Cycle: $CYCLE\]" scalaris_log4erl.txt | grep -oh "<[0-9]*\.[0-9]*\.[0-9]*> " | sort | uniq | tr -d '\n')
if [ -z Nodes ]; then
    echo "no compatible entries in logfile"
    exit 1
fi
declare -a nodes=( $Nodes )
length=${#nodes[@]}

# Calculate node positions (as a circle)
# Using the pos attribute is the only way to fix node positions between multiple
# graphs (layout=circo produces similar looking results, but the node positions
# change depending on the edges)
pi=$(echo "scale=10; 4*a(1)" | bc -l)
for i in $(seq 1 $length); do
    x=$(echo "c((($pi*2)/$length)*$i)*$RADIUS" | bc -l)
    y=$(echo "s((($pi*2)/$length)*$i)*$RADIUS" | bc -l)
    index=$(echo "$i-1"|bc)
    echo "    $(echo "${nodes[$index]}[pos=\"$x,$y!\"]");" >> $FILENAME.gv
done
echo -e "\n" >> $FILENAME.gv

# Insert the edges from the print_cache_log() output in the logfile
grep "\[Cycle: $CYCLE\]" $LOGFILE | sed "s/.*Cycle: $CYCLE] /    /" >> $FILENAME.gv

# close the graph
echo "}" >> $FILENAME.gv

# create pdf
dot -Tpdf $FILENAME.gv -o $FILENAME.pdf

# delete gv files
if [ $KEEP_GV_FILE = false ]; then
    rm $FILENAME.gv
fi
