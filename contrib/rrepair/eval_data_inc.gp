#! /usr/bin/gnuplot
# USED PARAMS
#	colDefFile -> file with column definitions
#	srcFile1 -> source file 1 path
#	srcFile1_title -> prepend this to titles of lines from srcFile1
#	srcFile2 -> source file 2 path (optional)
#	srcFile2_title -> prepend this to titles of lines from srcFile2 (optional unless srcFile2 given)
#	srcFile3 -> source file 3 path (optional)
#	srcFile3_title -> prepend this to titles of lines from srcFile3 (optional unless srcFile3 given)
#	srcFile4 -> source file 4 path (optional)
#	srcFile4_title -> prepend this to titles of lines from srcFile4 (optional unless srcFile4 given)
#	destDir -> destination path for pdfs
#	regenAccInPercent -> whether to show the regen accuracy in percent instead of absolute values (useful for bloom)
#	absoluteRedundancy -> whether to show the absolute redundancy or a relative one

set macro

load colDefFile

plotCount = exists("srcFile4") ? 4 : exists("srcFile3") ? 3 : exists("srcFile2") ? 2 : 1
files = srcFile1 . (plotCount >= 2 ? " " . srcFile2 : "") . (plotCount >= 3 ? " " . srcFile3 : "") . (plotCount >= 4 ? " " . srcFile4 : "")
get_file(i) = (i == 4) ? srcFile4 : (i == 3) ? srcFile3 : (i == 2) ? srcFile2 : srcFile1
get_title(i) = (i == 4) ? srcFile4_title : (i == 3) ? srcFile3_title : (i == 2) ? srcFile2_title : srcFile1_title
if (plotCount == 4) {
  set style line 1 lw 3 lc -1 pt 5 lt 1
  set style line 2 lw 3 lc 1 pt 9 lt 1
  set style line 3 lw 3 lc rgb "#32CD32" pt 7 lt 1
  set style line 4 lw 3 lc 3 pt 4 lt 3
  set style line 101 lw 3 lc -1 pt 0 lt 1
  set style line 102 lw 3 lc 1 pt 0 lt 1
  set style line 103 lw 3 lc rgb "#32CD32" pt 0 lt 1
  set style line 104 lw 3 lc 3 pt 0 lt 1
  plotShift(x, i) = (i == 4) ? (x + (x / 1.9)) : (i == 3) ? (x + (x / 6.5)) : (i == 2) ? (x - (x / 7.9)) : (x - (x / 2.95))
} else {
  set style line 1 lw 3 lc 1 pt 5 lt 1
  set style line 2 lw 3 lc rgb "#32CD32" pt 9 lt 1
  set style line 3 lw 3 lc 3 pt 7 lt 1
  set style line 101 lw 3 lc 1 pt 0 lt 1
  set style line 102 lw 3 lc rgb "#32CD32" pt 0 lt 1
  set style line 103 lw 3 lc 3 pt 0 lt 1
  if (plotCount == 3) {
    plotShift(x, i) = (i == 3) ? (x + (x / 2.3)) : (i == 2) ? (x) : (x - (x / 3.2))
  } else {
    if (plotCount == 2) {
      plotShift(x, i) = (i == 2) ? (x + (x / 3)) : (x - (x / 4))
    } else {
      plotShift(x, i) = x
    }
  }
}
set style line 5 lw 3 lc rgb "#777777" pt 8 lt 3
if (exists("regenAccInPercent") && regenAccInPercent == 1) {
  regenAcc(found, existing) = 100 - 100 * found / existing
  regenAccErr(found1, found2, existingSum) = stderrSum(found1, found2) * (100 / existingSum)
} else {
  regenAccInPercent = 0
  regenAcc(found, existing) = existing - found
  regenAccErr(found1, found2, existingSum) = stderrSum(found1, found2)
}
if (exists("absoluteRedundancy") && absoluteRedundancy == 1) {
  redundancy(transferred, updated, regen) = transferred - (updated + regen)
  redundancyStderr(transferred, updated, regen) = sqrt(transferred**2 - updated**2 - regen**2)
} else {
  absoluteRedundancy = 0
  redundancy(transferred, updated, regen) = transferred / (updated + regen)
  redundancyStderr(transferred, updated, regen) = "-"
}
plot_boxwidth = (0.8 / plotCount)

# OUTPUT
set terminal pdfcairo dashed enhanced font ",15" fontscale 0.5 size 6,4.5
fileEx = "pdf"

system "echo 'PLOT " . files . "'"
system "mkdir -p " .destDir

filename = "file"

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

acc_upd_max=0.5
if (regenAccInPercent == 1) {
  do for [i=1:plotCount] {
    stats "<awk '$" . col_ftype . " == \"update\"' " . get_file(i) u (column(col_missing)+column(col_outdated) - column(col_regen)-column(col_updated)+stderrSum(column(col_sd_regen),column(col_sd_updated))) nooutput
    if (STATS_max > acc_upd_max) {acc_upd_max = STATS_max}
  }
  acc_reg_max=50.5
  acc_reg_min=49.5
  do for [i=1:plotCount] {
    stats "<awk '$" . col_ftype . " == \"regen\"' " . get_file(i) u (regenAcc(column(col_regen)+column(col_updated), column(col_missing)+column(col_outdated))+regenAccErr(column(col_sd_regen),column(col_sd_updated),(column(col_missing)+column(col_outdated)))):(regenAcc(column(col_regen)+column(col_updated), column(col_missing)+column(col_outdated))-regenAccErr(column(col_sd_regen),column(col_sd_updated),(column(col_missing)+column(col_outdated)))) nooutput
    if (STATS_min_y < acc_reg_min) {acc_reg_min = STATS_min_y}
    if (STATS_max_x > acc_reg_max) {acc_reg_max = STATS_max_x}
  }
  acc_reg_avg=(acc_reg_max+acc_reg_min)/2.0
  acc_reg_max=max(abs(acc_reg_avg-acc_reg_min), abs(acc_reg_max-acc_reg_avg))
} else {
  do for [i=1:plotCount] {
    stats "<awk '$" . col_ftype . " == \"update\" || $" . col_ftype . " == \"regen\"' " . get_file(i) u (column(col_missing)+column(col_outdated) - column(col_regen)-column(col_updated)+stderrSum(column(col_sd_regen),column(col_sd_updated))) nooutput
    if (STATS_max > acc_upd_max) {acc_upd_max = STATS_max}
  }
  acc_reg_max = acc_upd_max
}
if (acc_upd_max > 0.5) {acc_upd_max=acc_upd_max*1.05}
if (acc_reg_max > 0.5) {acc_reg_max=acc_reg_max*1.05}

red_max=0.5
do for [i=1:plotCount] {
  stats get_file(i) using (redundancy(column(col_bw_rs_kvv), column(col_updated), column(col_regen))+redundancyStderr(column(col_sd_bw_rs_kvv), column(col_sd_updated), column(col_sd_regen))) nooutput
  if (STATS_max > red_max) {red_max = STATS_max}
}
if (red_max > 0.5) {red_max=red_max*1.05}
if (red_max >= 1.5 && red_max < 2) {red_max=2}

bw_max=1
do for [i=1:plotCount] {
  stats get_file(i) using (kB(column(col_bw_rc_size)+column(col_bw_rc2_size)+stderrSum(column(col_sd_bw_rc_size), column(col_sd_bw_rc2_size)))) nooutput
  if (STATS_max > bw_max) {bw_max = STATS_max}
}
# is naive solution more expensive?
stats get_file(1) using (kB(column(col_dbsize)/4*(128+32)/8)) nooutput
if (STATS_max > bw_max) {bw_max = STATS_max}

key_width_fun(i) = min(-0.5, (6-strstrt(get_title(i), "_")) - (strlen(get_title(i)) - strstrt(get_title(i), "_")) * 0.15)
key_width = min(key_width_fun(1),key_width_fun(plotCount))
#print "key_width: ",key_width

# Solid Background
# set object 1 rect from screen 0, 0, 0 to screen 1, 1, 0 behind
# set object 1 rect fc  rgb "white"  fillstyle solid 1.0

# Style
if (plotCount == 1) {
  set style data yerrorlines
} else {
  set style data yerrorbars
}
set logscale x 2
set xrange [2:8192] # TODO: adapt if number of data points changes!
set grid layerdefault linetype 0 linewidth 1.000,  linetype 0 linewidth 1.000
set pointsize (plotCount == 4) ? 0.7 : (plotCount == 3) ? 0.8 : 1
arrow_dx=.166666666667 # grid spacing in x
arrow_di=0 # added to each i in loop - arrows drawn at (i+arrow_di)*arrow_dx
arrow_xpoints=6 # TODO: adapt if number of data points changes!

set style line 100 lw 3 lc -1 pt 0 lt 1

#--------------------------------- ALL
set boxwidth plot_boxwidth relative
set style fill solid 0.75 noborder
set xtics scale 0.1
set ytics scale 0.8
set grid noxtics nomxtics layerdefault linetype 0 linewidth 1.000,  linetype 0 linewidth 1.000
set for [i=1:arrow_xpoints] arrow from graph (i+arrow_di)*arrow_dx,graph 0 to graph (i+arrow_di)*arrow_dx,graph 1 nohead back linetype 0 linewidth 1.000

set o destDir. "all_" .filename. "." .fileEx
set multiplot

all_width_r = 0.545
all_width_l = all_width_r
bw_height_full = 0.700
bw_height = 0.023 + ((plotCount > 1) ? (bw_height_full - 0.018) : bw_height_full)
red_height = (plotCount > 1) ? 0.170 : 0.174
red_pos_y = (plotCount > 1) ? bw_height_full - 0.033 : bw_height_full - 0.035
acc_height_full = (plotCount > 1) ? 0.200 : 0.204
acc_height = (plotCount > 1) ? (acc_height_full - 0.012) : acc_height_full
acc_pos_y = (plotCount > 1) ? (red_pos_y + red_height - 0.007) : (red_pos_y + red_height - 0.025)

# accuracy

set size all_width_l,acc_height
set origin -0.002,acc_pos_y
unset xlabel
set format x ""
set ylabel "|Δ| missed " font ",16"
set yrange [0:acc_upd_max]
set format y " %2.1f"
if (plotCount > 1) {
  set key at screen 0.512,(acc_pos_y + 0.001) center center vertical Left reverse opaque enhanced autotitles nobox maxrows 1 width (key_width+1) samplen 1.75 font ",14" spacing 1.3
} else {
  set key top left horizontal Left reverse opaque enhanced autotitles box maxcols 1 width key_width samplen 1.5 font ",13"
}

plot for [i=1:plotCount] "<awk '$" . col_ftype . " == \"update\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(column(col_missing)+column(col_outdated) - column(col_regen)-column(col_updated)):(stderrSum(column(col_sd_regen),column(col_sd_updated))) t get_title(i) ls i

set origin 0.49,acc_pos_y
unset key
unset ylabel
unset ytics
set grid y2tics
set size all_width_r,acc_height
if (regenAccInPercent == 1) {
  set y2tics mirror format "%-2.0f_{ }%%" offset 0 scale 0.8
  set y2range [(acc_reg_avg-acc_reg_max):(acc_reg_avg+acc_reg_max)]
} else {
  set y2tics mirror format "%-1.1f " scale 0.8
  set y2range [0:acc_reg_max]
}

plot for [i=1:plotCount] "<awk '$" . col_ftype . " == \"regen\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(regenAcc(column(col_regen)+column(col_updated), column(col_missing)+column(col_outdated))):(regenAccErr(column(col_sd_regen),column(col_sd_updated),(column(col_missing)+column(col_outdated)))) axes x1y2 t get_title(i) ls (plotCount > 1 ? i : 2)

set ytics scale 0.8
unset y2tics
set grid noy2tics

# redundancy

set ytics autofreq
set size all_width_l,red_height
set origin -0.002,red_pos_y
if (absoluteRedundancy == 1) {
set ylabel "Red." font ",16" # transferred / updated
} else {
set ylabel "rel. Red." font ",16" # transferred / updated
}
set yrange [0:red_max]
set y2range [0:red_max]
if (red_max > 10) {
  set format y "  %1.0f"
  set format y2 "%-1.0f "
} else {
  set format y " %1.1f"
  set format y2 "%-1.1f "
}
unset key

plot for [i=1:plotCount] "<awk '$" . col_ftype . " == \"update\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(redundancy(column(col_bw_rs_kvv), column(col_updated), column(col_regen))) with boxes t get_title(i) ls i, \
     for [i=1:plotCount] "<awk '$" . col_ftype . " == \"update\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(redundancy(column(col_bw_rs_kvv), column(col_updated), column(col_regen))):(redundancyStderr(column(col_sd_bw_rs_kvv), column(col_sd_updated), column(col_sd_regen))) with yerrorbars notitle ls (plotCount > 1 ? (100+i) : 100)

set size all_width_r,red_height
set origin 0.49,red_pos_y
if (plotCount == 1) {
  set key bottom right horizontal Right noreverse opaque enhanced autotitles box maxcols 1 width key_width samplen 1.5 font ",13"
}
unset ylabel
unset ytics
set grid y2tics
set y2tics mirror scale 0.8

plot for [i=1:plotCount] "<awk '$" . col_ftype . " == \"regen\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(redundancy(column(col_bw_rs_kvv), column(col_updated), column(col_regen))) axes x1y2 with boxes t get_title(i) ls (plotCount > 1 ? i : 2), \
     for [i=1:plotCount] "<awk '$" . col_ftype . " == \"regen\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(redundancy(column(col_bw_rs_kvv), column(col_updated), column(col_regen))):(redundancyStderr(column(col_sd_bw_rs_kvv), column(col_sd_updated), column(col_sd_regen))) with yerrorbars notitle ls (plotCount > 1 ? (100+i) : 100)

set ytics scale 0.8
unset y2tics
set grid noy2tics

# bandwidth

set size all_width_l,bw_height
set origin -0.002,-0.025
set xlabel "n in 10^3, update" font ",16"
set ylabel "RC costs (phase 1+2) in KiB" font ",16"
set yrange [1:bw_max*1.2]
set y2range [1:bw_max*1.2]
set logscale y 2
set logscale y2 2
set format y "%4.0f"
set format x
if (plotCount > 1) {
  set key at screen 0.512,(red_pos_y + 0.0065) center center vertical Left reverse opaque enhanced autotitles nobox maxrows 1 width (key_width+1) samplen 1.75 font ",14" spacing 1.3
} else {
  set key top left horizontal Left reverse opaque enhanced autotitles box maxcols 1 width key_width samplen 1.5 font ",13"
}

LABEL = "naïve approach"
set label 10 at 22,112000*(128+32)/8/1024 LABEL front center font ",12" textcolor rgb "#777777"

plot for [i=1:plotCount] "<awk '$" . col_ftype . " == \"update\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(kB(column(col_bw_rc_size)+column(col_bw_rc2_size))) with boxes notitle ls i fs solid 0.3, \
     for [i=1:plotCount] "<awk '$" . col_ftype . " == \"update\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(kB(column(col_bw_rc_size))) with boxes t get_title(i) ls i, \
     for [i=1:plotCount] "<awk '$" . col_ftype . " == \"update\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(kB(column(col_bw_rc_size)+column(col_bw_rc2_size))):(kB(stderrSum(column(col_sd_bw_rc_size),column(col_sd_bw_rc2_size)))) with yerrorlines notitle ls (plotCount > 1 ? (100+i) : 100), \
     "<awk '$" . col_ftype . " == \"update\"' " . get_file(1) \
 u (plotShift(column(col_dbsize)/4/1000, 2)):(kB(column(col_dbsize)/4*(128+32)/8)) with linespoints notitle ls 5

set size all_width_r,bw_height
set origin 0.49,-0.025
set xlabel "n in 10^3, regen" font ",16"
unset key
unset ylabel
unset ytics
set grid y2tics
set y2tics mirror format "%-4.0f" scale 0.8

plot for [i=1:plotCount] "<awk '$" . col_ftype . " == \"regen\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(kB(column(col_bw_rc_size)+column(col_bw_rc2_size))) axes x1y2 with boxes notitle ls (plotCount > 1 ? i : 2) fs solid 0.3, \
     for [i=1:plotCount] "<awk '$" . col_ftype . " == \"regen\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(kB(column(col_bw_rc_size))) axes x1y2 with boxes t get_title(i) ls (plotCount > 1 ? i : 2), \
     for [i=1:plotCount] "<awk '$" . col_ftype . " == \"regen\"' " . get_file(i) \
 u (plotShift(column(col_dbsize)/4/1000, i)):(kB(column(col_bw_rc_size)+column(col_bw_rc2_size))):(kB(stderrSum(column(col_sd_bw_rc_size),column(col_sd_bw_rc2_size)))) axes x1y2 with yerrorlines notitle ls (plotCount > 1 ? (100+i) : 100), \
     "<awk '$" . col_ftype . " == \"update\"' " . get_file(1) \
 u (plotShift(column(col_dbsize)/4/1000, 2)):(kB(column(col_dbsize)/4*(128+32)/8)) with linespoints notitle ls 5
