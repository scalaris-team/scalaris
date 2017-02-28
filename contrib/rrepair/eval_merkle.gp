#! /usr/bin/gnuplot
# USED PARAMS
#	colDefFile -> file with column definitions
#	srcFile1 -> source file 1 path
#	srcFile1_title -> prepend this to titles of lines from srcFile1
#	destDir -> destination path for pdfs
#	absoluteRedundancy -> whether to show the absolute redundancy or a relative one
#	maxMerkleBranch -> maximum merkle branch factor to show
#	maxMerkleBucket -> maximum merkle bucket size to show
#	filename -> defaults to "all_file" (no extension, .pdf will be appended)

set macro

load colDefFile

plotCount=1
files = srcFile1
get_file(i) = srcFile1
get_title(i) = srcFile1_title

# http://colorbrewer2.org/?type=qualitative&scheme=Set1&n=5
# (#e41a1c, #377eb8, #4daf4a, #984ea3, #ff7f00)
# -> a little brighter version:
set style line 1 lw 1 lt 1 lc rgb '#ff191b' pt 5 # dark red
set style line 2 lw 1 lt 1 lc rgb '#2d8ede' pt 9 # dark blue
set style line 3 lw 1 lt 1 lc rgb '#59c955' pt 7 # dark green
set style line 4 lw 1 lt 1 lc rgb '#b05abd' pt 4 # dark purple
set style line 5 lw 1 lt 1 lc rgb '#ff7f00' pt 10 # dark orange

set style line 101 lw 1 lt 1 lc rgb '#ff191b' pt 0 # dark red
set style line 102 lw 1 lt 1 lc rgb '#2d8ede' pt 0 # dark blue
set style line 103 lw 1 lt 1 lc rgb '#59c955' pt 0 # dark green
set style line 104 lw 1 lt 1 lc rgb '#b05abd' pt 0 # dark purple
set style line 105 lw 1 lt 1 lc rgb '#ff7f00' pt 0 # dark orange

regenAcc(found, existing) = existing - found
regenAccErr(found1, found2, existingSum) = stderrSum(found1, found2)
if (exists("absoluteRedundancy") && absoluteRedundancy == 1) {
  redundancy(transferred, updated, regen) = transferred - (updated + regen)
  redundancyStderr(transferred, updated, regen) = sqrt(transferred**2 - updated**2 - regen**2)
} else {
  absoluteRedundancy = 0
  redundancy(transferred, updated, regen) = transferred / (updated + regen)
  redundancyStderr(transferred, updated, regen) = "-"
}
if (!exists("maxMerkleBranch")) {
  maxMerkleBranch=16
}
if (!exists("maxMerkleBucket")) {
  maxMerkleBucket=16
}
plot_boxwidth = 0.8

# OUTPUT
set terminal pdfcairo dashed enhanced font ",15" fontscale 0.5 size 6,4.0
fileEx = "pdf"

system "echo 'PLOT " . files . "'"
system "mkdir -p " .destDir

if (!exists("filename")) {
  filename = "all_file"
}

# --OPTION
kvvSize0 = 16+4 # key+version size in bytes
kvvSize1 = kvvSize0+10 # key+version+value size in byte

# Helper
stderrSum(x,y) = sqrt(x**2 + y**2)	# sum of std. errors is root of sum of square errors
kB(x) = x / 1024
min(x,y) = (x < y) ? x : y
min3(x,y,z) = min(min(x,y),z)
max(x,y) = (x > y) ? x : y
max3(x,y,z) = max(max(x,y),z)

latency_font_size = min(16, 160 / max(maxMerkleBranch, maxMerkleBucket))

bw_max=1
bw_min=1.0e18
stats "<awk '($" . col_ftype . " == \"update\" || $" . col_ftype . " == \"regen\") && $" . col_merkle_branch . " <= " . maxMerkleBranch . " && $" . col_merkle_bucket . " <= " . maxMerkleBucket . "' " . get_file(i) using (kB(column(col_bw_rc_size)+column(col_bw_rc2_size))) nooutput
if (STATS_max > bw_max) {bw_max = STATS_max}
if (STATS_min < bw_min) {bw_min = STATS_min}

lat_max=1
lat_min=1.0e18
stats "<awk '($" . col_ftype . " == \"update\" || $" . col_ftype . " == \"regen\") && $" . col_merkle_branch . " <= " . maxMerkleBranch . " && $" . col_merkle_bucket . " <= " . maxMerkleBucket . "' " . get_file(i) using (column(col_bw_rc_msg)+column(col_bw_rc2_msg)) nooutput
if (STATS_max > lat_max) {lat_max = STATS_max}
if (STATS_min < lat_min) {lat_min = STATS_min}

key_width_fun(i) = min(-0.5, (9-strstrt(get_title(i), "_")) - (strlen(get_title(i)) - strstrt(get_title(i), "_")) * 0.25)
key_width = key_width_fun(1)
# print "key_width: ",key_width

# Solid Background
# set object 1 rect from screen 0, 0, 0 to screen 1, 1, 0 behind
# set object 1 rect fc  rgb "white"  fillstyle solid 1.0

# Style
set style data yerrorlines
set grid layerdefault   linetype 0 linewidth 1.000,  linetype 0 linewidth 1.000
set pointsize 1

set style line 100 lw 2 lc -1 pt 0 lt 1

#--------------------------------- ALL
set xrange [1.5:(maxMerkleBranch+0.5)]
set link y2
set yrange [0.5:(maxMerkleBucket+0.5)]
set boxwidth plot_boxwidth relative
set style fill solid 1 border -1
set xtics out scale 0.8 rotate by -30 offset -1.0,0.25
set ytics out mirror scale 0.8 1,2 offset 0.75
set mytics 2
set mxtics 2
set grid noxtics mxtics layerdefault linetype 0 linewidth 1.000,  linetype 0 linewidth 1.000
#set multiplot title "Transfer costs (phase 1+2) in KiB" font ",18"
unset key

all_width = 0.545
bw_height = 0.875

# bandwidth outdated+missing, heatmap=costs, text labels=hops
set o destDir . filename . "." . fileEx
set multiplot

set view map scale 1
# set palette rgbformula -7,2,-7
set colorbox horizontal user origin 0.025,0.042 size 0.95,0.04
set cbrange [floor(bw_min):ceil(bw_max)]
# set cblabel "Transfer costs (phase 1+2) in KiB"
# set cblabel "in KiB:" offset -38,4.3
set cblabel "Transfer costs in KiB (white text labels = number of hops):" left offset screen -0.13,0.165
set cbtics offset 0,0.5
# unset cbtics

set size all_width,bw_height
set origin 0,0.14
# set title "update" offset 0,-0.5 font ",17"
set xlabel "branch factor v (outdated δ)" font ",16" offset 0,0.45
set ylabel "bucket size b" font ",16" offset 2.0,0.0

plot "<awk '$" . col_ftype . " == \"update\" && $" . col_merkle_branch . " <= " . maxMerkleBranch . " && $" . col_merkle_bucket . " <= " . maxMerkleBucket . "' " . get_file(i) \
 u (column(col_merkle_branch)):(column(col_merkle_bucket)):(kB(column(col_bw_rc_size)+column(col_bw_rc2_size))) notitle with image, \
      "" \
 u (column(col_merkle_branch)):(column(col_merkle_bucket)):(sprintf("%2.0f", column(col_bw_rc_msg)+column(col_bw_rc2_msg))) notitle with labels tc "white" font ",".latency_font_size

set size (all_width+0.025),bw_height
set origin 0.49,0.14
# set title "regen" offset 0,-0.5 font ",17"
set xlabel "branch factor v (missing δ)" font ",16" offset 0,0.45
# set rmargin at screen 0.924
unset ylabel
# unset ytics
set ytics (0)
set y2tics out mirror scale 0.8 1,2 offset -0.75
set my2tics 2
unset colorbox

plot "<awk '$" . col_ftype . " == \"regen\" && $" . col_merkle_branch . " <= " . maxMerkleBranch . " && $" . col_merkle_bucket . " <= " . maxMerkleBucket . "' " . get_file(i) \
 u (column(col_merkle_branch)):(column(col_merkle_bucket)):(kB(column(col_bw_rc_size)+column(col_bw_rc2_size))) notitle with image, \
      "" \
 u (column(col_merkle_branch)):(column(col_merkle_bucket)):(sprintf("%2.0f", column(col_bw_rc_msg)+column(col_bw_rc2_msg))) notitle with labels tc "white" font ",".latency_font_size
 
# bandwidth outdated, heatmap left=costs, heatmap right=hops
unset multiplot
set o destDir . filename . "-outdated." . fileEx
set multiplot

set view map scale 1
# set palette rgbformula -7,2,-7

set size all_width,bw_height
set origin 0,0.14
# set title "update" offset 0,-0.5 font ",17"
unset y2tics
set ytics out mirror scale 0.8 1,2 offset 0.75
set xlabel "branch factor v (outdated δ)" font ",16" offset 0,0.45
set ylabel "bucket size b" font ",16" offset 2.0,0.0

set colorbox horizontal user origin graph 0.0,graph -0.27 size graph 1,graph 0.04
set cbrange [floor(bw_min):ceil(bw_max)]
set cblabel "Transfer costs in KiB:" offset screen 0.00,0.150
set cbtics offset 0,0.3

plot "<awk '$" . col_ftype . " == \"update\" && $" . col_merkle_branch . " <= " . maxMerkleBranch . " && $" . col_merkle_bucket . " <= " . maxMerkleBucket . "' " . get_file(i) \
 u (column(col_merkle_branch)):(column(col_merkle_bucket)):(kB(column(col_bw_rc_size)+column(col_bw_rc2_size))) notitle with image

set size (all_width+0.025),bw_height
set origin 0.49,0.14
# set title "regen" offset 0,-0.5 font ",17"
set xlabel "branch factor v (outdated δ)" font ",16" offset 0,0.45
# set rmargin at screen 0.924
unset ylabel
# unset ytics
set ytics (0)
set y2tics out mirror scale 0.8 1,2 offset -0.75
set my2tics 2
set cbrange [floor(lat_min):ceil(lat_max)]
set cblabel "Number of hops:"
set cbtics offset 0,0.3

plot "<awk '$" . col_ftype . " == \"update\" && $" . col_merkle_branch . " <= " . maxMerkleBranch . " && $" . col_merkle_bucket . " <= " . maxMerkleBucket . "' " . get_file(i) \
 u (column(col_merkle_branch)):(column(col_merkle_bucket)):(column(col_bw_rc_msg)+column(col_bw_rc2_msg)) notitle with image
 
# bandwidth missing, heatmap=costs, text labels=hops
unset multiplot
set o destDir . filename . "-missing." . fileEx
set multiplot

set view map scale 1
# set palette rgbformula -7,2,-7

set size all_width,bw_height
set origin 0,0.14
# set title "update" offset 0,-0.5 font ",17"
unset y2tics
set ytics out mirror scale 0.8 1,2 offset 0.75
set xlabel "branch factor v (outdated δ)" font ",16" offset 0,0.45
set ylabel "bucket size b" font ",16" offset 2.0,0.0
set colorbox horizontal user origin graph 0.0,graph -0.27 size graph 1,graph 0.04
set cbrange [floor(bw_min):ceil(bw_max)]
set cblabel "Transfer costs in KiB:" offset screen 0.00,0.150
set cbtics offset 0,0.3

plot "<awk '$" . col_ftype . " == \"regen\" && $" . col_merkle_branch . " <= " . maxMerkleBranch . " && $" . col_merkle_bucket . " <= " . maxMerkleBucket . "' " . get_file(i) \
 u (column(col_merkle_branch)):(column(col_merkle_bucket)):(kB(column(col_bw_rc_size)+column(col_bw_rc2_size))) notitle with image

set size (all_width+0.025),bw_height
set origin 0.49,0.14
# set title "regen" offset 0,-0.5 font ",17"
set xlabel "branch factor v (outdated δ)" font ",16" offset 0,0.45
# set rmargin at screen 0.924
unset ylabel
# unset ytics
set ytics (0)
set y2tics out mirror scale 0.8 1,2 offset -0.75
set my2tics 2
set cbrange [floor(lat_min):ceil(lat_max)]
set cblabel "Number of hops:"
set cbtics offset 0,0.3

plot "<awk '$" . col_ftype . " == \"regen\" && $" . col_merkle_branch . " <= " . maxMerkleBranch . " && $" . col_merkle_bucket . " <= " . maxMerkleBucket . "' " . get_file(i) \
 u (column(col_merkle_branch)):(column(col_merkle_bucket)):(column(col_bw_rc_msg)+column(col_bw_rc2_msg)) notitle with image
