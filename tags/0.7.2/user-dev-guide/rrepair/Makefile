MAKEFLAGS=-k $MAKEFLAGS
GNUPLOT=gnuplot
PIC2PLOT=pic2plot -Tps
GNU_FILES := $(patsubst %,.%-done,$(wildcard *.gnuplot))
PIC_FILES := $(patsubst %.pic,%.pdf,$(wildcard *.pic))

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

sequence.pdf: sequence.pic
	

%.ps: %.pic sequence.pic
	$(PIC2PLOT) $< > $@
#	note: if pic2plot is broken (as in openSUSE 12.3), use this with some caveats, e.g. subscript not working:
# 	pic $< | groff > $@

%.pdf: %.ps
	epstopdf $*.ps > $@ && rm $*.ps

clean:
	rm -f *.png
	rm -f *.pdf
	rm -f *.ps
	rm -f .*.gnuplot-done
	rm -f *-fit_quadratic
	rm -f *-fit_linear
	rm -f *.pic.ps
	rm -f *.pic.pdf
