#! /usr/bin/gnuplot
# USED PARAMS
#	colDefFile -> file with column definitions
#	srcFile1 -> source file 1 path
#	srcFile1_title -> prepend this to titles of lines from srcFile1
#	destDir -> destination path for pdfs
#	stepSize -> the stepSize parameter used
#	plot_label -> additional label to print at the bottom left of the screen (optional)
#	RC_costs_note -> replaces "phase 1+2" in the y-axis description of the transfer costs (optional)
#	filename -> defaults to "all_file" (no extension, .pdf will be appended)

set macro

load colDefFile

step_size = exists("stepSize") ? stepSize : 2
plotShift(x, i) = (i == 3) ? (x + 0.55*(0.5*step_size)) : (i == 2) ? (x) : (x - 0.55*(0.5*step_size))
# do not use relative accuracy or redundancy (for now) since we already show (absolute) differences and the values are low
regenAccInPercent = 0
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
plot_boxwidth = (0.8 / 3)

# OUTPUT
set terminal pdfcairo dashed enhanced font ",15" fontscale 0.5 size 6,4.5
fileEx = "pdf"

system "echo 'PLOT ".srcFile1."'"
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

acc_min=-0.25
acc_max=0.25
stats "<awk '($" . col_ftype . " == \"update\" || $" . col_ftype . " == \"regen\") {Acc[$" . col_ftype . "][$" . col_failrate . "][$" . col_ddist . "\",\"$" . col_fdist . "]=$" . col_missing . "\" \"$" . col_outdated . "\" \"$" . col_regen . "\" \"$" . col_updated . "} END{for (i in Acc) {for (j in Acc[i]) {for (k in Acc[i][j]) {print i,j,Acc[i][j][\"random,random\"],Acc[i][j][k]}}} }' " . srcFile1 using (($7+$8 - $9-$10)-($3+$4 - $5-$6)) nooutput
if (STATS_min < acc_min) {acc_min = STATS_min}
if (STATS_max > acc_max) {acc_max = STATS_max}
acc_max_abs=max(acc_max, -acc_min)
if (acc_max_abs > 0.25) {acc_max_abs=acc_max_abs*1.05}

red_min=-0.25
red_max=0.25
stats "<awk '($" . col_ftype . " == \"update\" || $" . col_ftype . " == \"regen\") {Red[$" . col_ftype . "][$" . col_failrate . "][$" . col_ddist . "\",\"$" . col_fdist . "]=$" . col_bw_rs_kvv . "\" \"$" . col_updated . "\" \"$" . col_regen . "} END{for (i in Red) {for (j in Red[i]) {for (k in Red[i][j]) {print i,j,Red[i][j][\"random,random\"],Red[i][j][k]}}} }' " . srcFile1 using (redundancy($6, $7, $8)-redundancy($3, $4, $5)) nooutput
if (STATS_min < red_min) {red_min = STATS_min}
if (STATS_max > red_max) {red_max = STATS_max}
red_max_abs=max(red_max, -red_min)
if (red_max_abs > 0.25) {red_max_abs=red_max_abs*1.05}

bw_min=-0.5
bw_max=0.5
stats "<awk '($" . col_ftype . " == \"update\" || $" . col_ftype . " == \"regen\") {BwRc[$" . col_ftype . "][$" . col_failrate . "][$" . col_ddist . "\",\"$" . col_fdist . "]+=$" . col_bw_rc_size . "+$" . col_bw_rc2_size . "} END{for (i in BwRc) {for (j in BwRc[i]) {for (k in BwRc[i][j]) {print i,100*(BwRc[i][j][k]/BwRc[i][j][\"random,random\"])-100}}} }' " . srcFile1 using 2 nooutput
if (STATS_min < bw_min) {bw_min = STATS_min}
if (STATS_max > bw_max) {bw_max = STATS_max}
bw_max_abs=max(bw_max, -bw_min)
if (bw_max_abs > 0.5) {
  bw_max_abs=bw_max_abs*1.05
  # leave at least 1/4 of the whole graph to each side (plus/minus)
  bw_min=-bw_min < bw_max_abs / 3 ? -bw_max_abs / 3 : bw_min*1.05
  bw_max=bw_max < bw_max_abs / 3 ? bw_max_abs / 3 : bw_max*1.05
}

largeRCdiff = (bw_max_abs >= 10) ? 1 : 0

# Solid Background
# set object 1 rect from screen 0, 0, 0 to screen 1, 1, 0 behind
# set object 1 rect fc  rgb "white"  fillstyle solid 1.0

# Style
set style data linespoint
set xtics 0,step_size,5*step_size
set grid layerdefault   linetype 0 linewidth 1.000,  linetype 0 linewidth 1.000

set style line 100 lw 1 lc -1 pt 0 lt 1

set style line 1 lw 1 lt 1 lc rgb '#ff191b' pt 5 # dark red
set style line 2 lw 1 lt 1 lc rgb '#2d8ede' pt 9 # dark blue
set style line 3 lw 1 lt 1 lc rgb '#59c955' pt 7 # dark green
set style line 4 lw 1 lt 1 lc rgb '#b05abd' pt 4 # dark purple
set style line 5 lw 1 lt 1 lc rgb '#ff7f00' pt 10 # dark orange

#--------------------------------- ALL
set xrange [(0.5*step_size):(5.5*step_size)]
set boxwidth plot_boxwidth relative
set style fill solid 1 border -1
set xtics scale 0.1
set mxtics 2
set grid noxtics mxtics layerdefault linetype 0 linewidth 1.000,  linetype 0 linewidth 1.000

set o destDir . filename . "." . fileEx
set multiplot

all_width_r = 0.545
all_width_l = all_width_r
bw_height_full = 0.700
bw_height = bw_height_full - 0.02
red_height = 0.180
red_pos_y = bw_height_full - 0.032
acc_height_full = 0.205
acc_height = acc_height_full
acc_pos_y = red_pos_y + red_height - 0.04

# accuracy

set size all_width_l,acc_height
set origin -0.002,acc_pos_y
unset xlabel
set format x ""
set ylabel "|Δ| missed " font ",16"
set yrange [-acc_max_abs:acc_max_abs]
set ytics mirror offset -2,0 right format (largeRCdiff ? "%+7.1f" : "%+6.1f") scale 0.8
unset key

plot "<awk '$" . col_ftype . " == \"update\" {Acc[$" . col_failrate . "][$" . col_ddist . "\",\"$" . col_fdist . "]=$" . col_missing . "\" \"$" . col_outdated . "\" \"$" . col_regen . "\" \"$" . col_updated . "} END{print \"#fprob rand,rand rand,bin_0.2 bin_0.2,rand bin_0.2,bin_0.2\" ; for (i in Acc) {print i,Acc[i][\"random,random\"],Acc[i][\"random,'\\''binomial_0.200000'\\''\"],Acc[i][\"'\\''binomial_0.200000'\\'',random\"],Acc[i][\"'\\''binomial_0.200000'\\'','\\''binomial_0.200000'\\''\"]} }' " . srcFile1 \
 u (plotShift($1,1)):(($6+$7 - $8-$9)-($2+$3 - $4-$5)) t "data_{rand}   , fail_{bin_{0.2 }}" with boxes ls 1, \
     "" \
 u (plotShift($1,2)):(($10+$11 - $12-$13)-($2+$3 - $4-$5)) t "data_{bin_{0.2 }} , fail_{rand}  " with boxes ls 2, \
     "" \
 u (plotShift($1,3)):(($14+$15 - $16-$17)-($2+$3 - $4-$5)) t "data_{bin_{0.2 }} , fail_{bin_{0.2 }" with boxes ls 3

set origin 0.49,acc_pos_y
unset key
unset ylabel
unset ytics
set grid y2tics
set size all_width_r,acc_height
set y2tics mirror offset (largeRCdiff ? 5 : 4),0 right format (largeRCdiff ? "%+7.1f" : "%+6.1f") scale 0.8
set y2range [-acc_max_abs:acc_max_abs]

plot "<awk '$" . col_ftype . " == \"regen\" {Acc[$" . col_failrate . "][$" . col_ddist . "\",\"$" . col_fdist . "]=$" . col_missing . "\" \"$" . col_outdated . "\" \"$" . col_regen . "\" \"$" . col_updated . "} END{print \"#fprob rand,rand rand,bin_0.2 bin_0.2,rand bin_0.2,bin_0.2\" ; for (i in Acc) {print i,Acc[i][\"random,random\"],Acc[i][\"random,'\\''binomial_0.200000'\\''\"],Acc[i][\"'\\''binomial_0.200000'\\'',random\"],Acc[i][\"'\\''binomial_0.200000'\\'','\\''binomial_0.200000'\\''\"]} }' " . srcFile1 \
 u (plotShift($1,1)):(($6+$7 - $8-$9)-($2+$3 - $4-$5)) t "data_{rand}   , fail_{bin_{0.2 }}" axes x1y2 with boxes ls 1, \
     "" \
 u (plotShift($1,2)):(($10+$11 - $12-$13)-($2+$3 - $4-$5)) t "data_{bin_{0.2 }} , fail_{rand}  " axes x1y2 with boxes ls 2, \
     "" \
 u (plotShift($1,3)):(($14+$15 - $16-$17)-($2+$3 - $4-$5)) t "data_{bin_{0.2 }} , fail_{bin_{0.2 }" axes x1y2 with boxes ls 3

unset y2tics
set grid noy2tics

# redundancy

set size all_width_l,red_height
set origin -0.002,red_pos_y
if (absoluteRedundancy == 1) {
set ylabel "Red." font ",16" # transferred / updated
} else {
set ylabel "rel. Red." font ",16" # transferred / updated
}
set yrange [-red_max_abs:red_max_abs]
set y2range [-red_max_abs:red_max_abs]
set ytics mirror offset -2,0 right format (largeRCdiff ? "%+7.1f" : "%+6.1f")
unset key

plot "<awk '$" . col_ftype . " == \"update\" {Red[$" . col_failrate . "][$" . col_ddist . "\",\"$" . col_fdist . "]=$" . col_bw_rs_kvv . "\" \"$" . col_updated . "\" \"$" . col_regen . "} END{print \"#fprob rand,rand rand,bin_0.2 bin_0.2,rand bin_0.2,bin_0.2\" ; for (i in Red) {print i,Red[i][\"random,random\"],Red[i][\"random,'\\''binomial_0.200000'\\''\"],Red[i][\"'\\''binomial_0.200000'\\'',random\"],Red[i][\"'\\''binomial_0.200000'\\'','\\''binomial_0.200000'\\''\"]} }' " . srcFile1 \
 u (plotShift($1,1)):(redundancy($5, $6, $7)-redundancy($2, $3, $4)) t "data_{rand}   , fail_{bin_{0.2 }}" with boxes ls 1, \
     "" \
 u (plotShift($1,2)):(redundancy($8, $9, $10)-redundancy($2, $3, $4)) t "data_{bin_{0.2 }} , fail_{rand}  " with boxes ls 2, \
     "" \
 u (plotShift($1,3)):(redundancy($11, $12, $13)-redundancy($2, $3, $4)) t "data_{bin_{0.2 }} , fail_{bin_{0.2 }" with boxes ls 3

set size all_width_r,red_height
set origin 0.49,red_pos_y
unset key
# set key bottom right horizontal Right noreverse opaque enhanced autotitles box maxcols 1 width -7 samplen 1.5 font ",13"
unset ylabel
unset ytics
set grid y2tics
set y2tics mirror offset (largeRCdiff ? 5 : 4),0 right format (largeRCdiff ? "%+7.1f" : "%+6.1f") scale 0.8

plot "<awk '$" . col_ftype . " == \"regen\" {Red[$" . col_failrate . "][$" . col_ddist . "\",\"$" . col_fdist . "]=$" . col_bw_rs_kvv . "\" \"$" . col_updated . "\" \"$" . col_regen . "} END{print \"#fprob rand,rand rand,bin_0.2 bin_0.2,rand bin_0.2,bin_0.2\" ; for (i in Red) {print i,Red[i][\"random,random\"],Red[i][\"random,'\\''binomial_0.200000'\\''\"],Red[i][\"'\\''binomial_0.200000'\\'',random\"],Red[i][\"'\\''binomial_0.200000'\\'','\\''binomial_0.200000'\\''\"]} }' " . srcFile1 \
 u (plotShift($1,1)):(redundancy($5, $6, $7)-redundancy($2, $3, $4)) t "data_{rand}   , fail_{bin_{0.2 }}" axes x1y2 with boxes ls 1, \
     "" \
 u (plotShift($1,2)):(redundancy($8, $9, $10)-redundancy($2, $3, $4)) t "data_{bin_{0.2 }} , fail_{rand}  " axes x1y2 with boxes ls 2, \
     "" \
 u (plotShift($1,3)):(redundancy($11, $12, $13)-redundancy($2, $3, $4)) t "data_{bin_{0.2 }} , fail_{bin_{0.2 }" axes x1y2 with boxes ls 3

unset y2tics
set grid noy2tics

# bandwidth

set size all_width_l,bw_height
set origin -0.002,0
set xlabel "total δ (outdated items)" font ",16"
set xtics 0,step_size,5*step_size format "%g_{ }%%" rotate by -30 offset -1,0
set ylabel sprintf("Transfer costs (%s)",exists("RC_costs_note") ? RC_costs_note : "phase 1+2") font ",16"
set yrange [bw_min:bw_max]
set y2range [bw_min:bw_max]
set ytics mirror offset 0,0 right format "%+1.1f_{ }%%" scale 0.8
set key at screen 0.500,(red_pos_y + 0.0065) center center vertical Left reverse opaque enhanced autotitles nobox maxrows 1 width -4 samplen 1.75 font ",14" spacing 1.4

plot "<awk '$" . col_ftype . " == \"update\" {BwRc[$" . col_failrate . "][$" . col_ddist . "\",\"$" . col_fdist . "]=$" . col_bw_rc_size . "+$" . col_bw_rc2_size . "} END{print \"#fprob rand,rand rand,bin_0.2 bin_0.2,rand bin_0.2,bin_0.2\" ; for (i in BwRc) {print i,BwRc[i][\"random,random\"],BwRc[i][\"random,'\\''binomial_0.200000'\\''\"],BwRc[i][\"'\\''binomial_0.200000'\\'',random\"],BwRc[i][\"'\\''binomial_0.200000'\\'','\\''binomial_0.200000'\\''\"]} }' " . srcFile1 \
 u (plotShift($1,1)):(100*($3/$2)-100) t "data_{rand}   , fail_{bin_{0.2 }}" with boxes ls 1, \
     "" \
 u (plotShift($1,2)):(100*($4/$2)-100) t "data_{bin_{0.2 }} , fail_{rand}  " with boxes ls 2, \
     "" \
 u (plotShift($1,3)):(100*($5/$2)-100) t "data_{bin_{0.2 }} , fail_{bin_{0.2 }" with boxes ls 3

set size all_width_r,bw_height
set origin 0.49,0
set xlabel "total δ (missing items)" font ",16"
unset ylabel
unset ytics
set grid y2tics
set y2tics mirror offset (largeRCdiff ? 7 : 6),0 right format "%+1.1f_{ }%%" scale 0.8

if (exists("plot_label") && strlen(plot_label) > 0) {
# label with box: (box width unreliable for enhanced text)
# set obj 100 rect at char (strlen(plot_label)/2.0+0.25),char 1 size char strlen(plot_label),char 1
# set obj 100 fillstyle empty border -1 front
unset label 100
set label 100 at char 1,char 1 plot_label front left font ",12"
}

plot "<awk '$" . col_ftype . " == \"regen\" {BwRc[$" . col_failrate . "][$" . col_ddist . "\",\"$" . col_fdist . "]=$" . col_bw_rc_size . "+$" . col_bw_rc2_size . "} END{print \"#fprob rand,rand rand,bin_0.2 bin_0.2,rand bin_0.2,bin_0.2\" ; for (i in BwRc) {print i,BwRc[i][\"random,random\"],BwRc[i][\"random,'\\''binomial_0.200000'\\''\"],BwRc[i][\"'\\''binomial_0.200000'\\'',random\"],BwRc[i][\"'\\''binomial_0.200000'\\'','\\''binomial_0.200000'\\''\"]} }' " . srcFile1 \
 u (plotShift($1,1)):(100*($3/$2)-100) axes x1y2 with boxes t "data_{rand}   , fail_{bin_{0.2 }}" ls 1, \
     "" \
 u (plotShift($1,2)):(100*($4/$2)-100) axes x1y2 with boxes t "data_{bin_{0.2 }} , fail_{rand}  " ls 2, \
     "" \
 u (plotShift($1,3)):(100*($5/$2)-100) axes x1y2 with boxes t "data_{bin_{0.2 }} , fail_{bin_{0.2 }" ls 3
