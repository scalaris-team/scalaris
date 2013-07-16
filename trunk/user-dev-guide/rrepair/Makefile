MAKEFLAGS=-k $MAKEFLAGS
GNUPLOT=gnuplot
PIC2PLOT=pic2plot -Tps
GNU_FILES := $(patsubst %,.%-done,$(wildcard *.gnuplot))
PIC_FILES := $(patsubst %,%.pdf,$(wildcard *.pic))

.PHONY: all files allSims clean

all: files

.DELETE_ON_ERROR:

files: $(GNU_FILES) $(PIC_FILES)

# convert all .gnuplot files to .png files
# % replaces any name
# within a rule: $< replaces the source
#                $@ replaces the target
.%.gnuplot-done: %.gnuplot data-scalability_get_10000_rate-SUMMARY-ALL.txt
	$(GNUPLOT) $<
	touch $@

sequence.pic.pdf: sequence.pic
	

%.pic.pdf: %.pic sequence.pic
	$(PIC2PLOT) $< | epstopdf --filter > $@
#	note: if pic2plot is broken (as in openSUSE 12.3), use this with some caveats, e.g. subscript not working:
# 	pic $< | groff | epstopdf --filter > $@

clean:
	rm -f *.png
	rm -f *.pdf
	rm -f .*.gnuplot-done
	rm -f *-fit_quadratic
	rm -f *-fit_linear
	rm -f *.pic.ps
	rm -f *.pic.pdf
