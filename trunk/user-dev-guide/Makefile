LATEX=pdflatex
FILE=main.tex
all:
	../bin/scalarisctl -h > scalarisctl-h.out
	../java-api/scalaris --noconfig --help > scalaris-client-java.out
	../python-api/scalaris --help > scalaris-client-python.out
	../ruby-api/scalaris --help > scalaris-client-ruby.out
	$(MAKE) -C rrepair
	$(LATEX) $(FILE) | awk -f ./latex-col.awk
	makeindex main
	$(LATEX) $(FILE) | awk -f ./latex-col.awk
	@while ( grep "Rerun to get cross-references" \
		$(subst .tex,.log, $(FILE)) > /dev/null ); do \
		echo '*** Going to Rerun LaTeX ***'; \
		$(LATEX) $(FILE) | awk -f ./latex-col.awk; \
	done

clean:
	rm -f *.aux *.ind *.ilg *.idx *.log *.out *.toc
