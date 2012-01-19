set xdata time
set timefmt "%Y-%m-%d:%H:%M:%S"
set terminal png font ",10pt"
set output "/usr/lib/scalaris/contrib/opennebula/public/hits_per_minute.png"
set boxwidth 1
set yrange [0:*]
set format x "%a\n%H:%M"
#set xtics 60
set ylabel "hits"
set xlabel "time"
set offsets 0,0,1,0
#set grid

plot '/tmp/result.by_date' using 1:2 with boxes notitle
