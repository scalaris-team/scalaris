SHELL := /bin/bash
MAKEFLAGS=-k $MAKEFLAGS
SUBDIRS := $(shell ls -d */ 2>/dev/null)
SUBDIRS_DIR := $(patsubst %/,%-dir,$(SUBDIRS))
SUBDIRS_DIR_CLEAN := $(patsubst %/,%-dir-clean,$(SUBDIRS))
SYSTEM_SIZE=100000
SYSTEM_SIZE_SCALE=4000
EVAL_REPEATS ?= 100

.PHONY: all clean subdirs
.PRECIOUS: %/.gitignore %/Makefile

all: subdirs

clean: $(SUBDIRS_DIR_CLEAN)

subdirs: $(SUBDIRS_DIR)

# % replaces any name
# within a rule: $@ inserts the target
#                $< inserts the first dependency (source)
#                $+ inserts a list of all dependencies
#                $^ inserts a list of all unique dependencies
#                $* inserts the stem an implicit rule matches with
%-dir: %
	$(MAKE) -C $<

%-dir-clean: %
	$(MAKE) clean -C $<

scalaris:
	$(MAKE) -C ../..

%/.gitignore: gitignore.tmpl2
	@mkdir -p "$(@D)"
	@cp gitignore.tmpl2 $@

# note: we must have the same dependencies as the specific rules
#       -> otherwise this rule may be chosen instead
%/Makefile: Makefile.tmpl %/.gitignore Makefile
	@mkdir -p "$(@D)"
	@cp $< $@
	@rm "$*/.gitignore"

# fprob analysis (different delta) for different trivial recon parameters
trivial_0-%-all/Makefile: Makefile.tmpl2 trivial_0-%-all/.gitignore Makefile
	@mkdir -p "$(@D)"
	@sed -e 's|<sys_size>|$(SYSTEM_SIZE)|g' \
         -e 's|<dat1>|0.001/trivial.dat|g' \
         -e 's|<dat2>|0.01/trivial.dat|g' \
         -e 's|<dat3>|0.1/trivial.dat|g' \
         -e 's|<title1>|trivial_{P_{1e}0.001}|g' \
         -e 's|<title2>|trivial_{P_{1e}0.01}|g' \
         -e 's|<title3>|trivial_{P_{1e}0.1}|g' \
         -e 's|<morecommands>|absoluteRedundancy=1;stepSize=$(shell expr $* / 5);|g' < $< > $@

trivial_0-10-all/%/trivial.dat: trivial_0-10-all/Makefile
	@echo " executing trivial_0-10 $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a trivial -n "$(SYSTEM_SIZE)" -c "$*,2" -g eval_fprob.gp -t "trivial_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

trivial_0-100-all/%/trivial.dat: trivial_0-100-all/Makefile
	@echo " executing trivial_0-100 $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a trivial -n "$(SYSTEM_SIZE)" -c "$*,20" -g eval_fprob.gp -t "trivial_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 20 > "$(@D)/eval.log"

trivial-all: trivial_0-10-all/0.1/trivial.dat trivial_0-10-all/0.01/trivial.dat trivial_0-10-all/0.001/trivial.dat trivial_0-100-all/0.1/trivial.dat trivial_0-100-all/0.01/trivial.dat trivial_0-100-all/0.001/trivial.dat
	

# ddist+fdist analysis (different data and failure distributions) for different trivial recon parameters
trivial_ddists_fdists-all/%/trivial_ddists_fdists.dat: trivial_ddists_fdists-all/Makefile
	@echo " executing trivial_ddists_fdists $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a trivial_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$*,2" -g eval_fprob_dists.gp -t "trivial_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

trivial_dists-all: trivial_ddists_fdists-all/0.01/trivial_ddists_fdists.dat
	

# scalability analysis (increasing data size) for different trivial recon parameters
trivial_scale-all/%/trivial_scale.dat: trivial_scale-all/Makefile
	@echo " executing trivial_scale $* (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a trivial_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$*" -g eval_data_inc.gp -t "trivial_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

trivial_scale-all: trivial_scale-all/0.01/trivial_scale.dat
	

trivial: trivial-all trivial_dists-all trivial_scale-all
	

# fprob analysis (different delta) for different shash recon parameters
shash_0-%-all/Makefile: Makefile.tmpl2 shash_0-%-all/.gitignore Makefile
	@mkdir -p "$(@D)"
	@sed -e 's|<sys_size>|$(SYSTEM_SIZE)|g' \
         -e 's|<dat1>|0.001/shash.dat|g' \
         -e 's|<dat2>|0.01/shash.dat|g' \
         -e 's|<dat3>|0.1/shash.dat|g' \
         -e 's|<title1>|shash_{P_{1e}0.001}|g' \
         -e 's|<title2>|shash_{P_{1e}0.01}|g' \
         -e 's|<title3>|shash_{P_{1e}0.1}|g' \
         -e 's|<morecommands>|absoluteRedundancy=1;stepSize=$(shell expr $* / 5);|g' < $< > $@

shash_0-10-all/%/shash.dat: shash_0-10-all/Makefile
	@echo " executing shash_0-10 $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a shash -n "$(SYSTEM_SIZE)" -c "$*,2" -g eval_fprob.gp -t "shash_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

shash_0-100-all/%/shash.dat: shash_0-100-all/Makefile
	@echo " executing shash_0-100 $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a shash -n "$(SYSTEM_SIZE)" -c "$*,20" -g eval_fprob.gp -t "shash_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 20 > "$(@D)/eval.log"

shash-all: shash_0-10-all/0.1/shash.dat shash_0-10-all/0.01/shash.dat shash_0-10-all/0.001/shash.dat shash_0-100-all/0.1/shash.dat shash_0-100-all/0.01/shash.dat shash_0-100-all/0.001/shash.dat
	

# ddist+fdist analysis (different data and failure distributions) for different shash recon parameters
shash_ddists_fdists-all/%/shash_ddists_fdists.dat: shash_ddists_fdists-all/Makefile
	@echo " executing shash_ddists_fdists $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a shash_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$*,2" -g eval_fprob_dists.gp -t "shash_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

shash_dists-all: shash_ddists_fdists-all/0.01/shash_ddists_fdists.dat
	

# scalability analysis (increasing data size) for different shash recon parameters
shash_scale-all/%/shash_scale.dat: shash_scale-all/Makefile
	@echo " executing shash_scale $* (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a shash_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$*" -g eval_data_inc.gp -t "shash_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

shash_scale-all: shash_scale-all/0.01/shash_scale.dat
	

shash: shash-all shash_dists-all shash_scale-all
	

# fprob analysis (different delta) for different bloom recon parameters
bloom_0-%-all/Makefile: Makefile.tmpl2 bloom_0-%-all/.gitignore Makefile
	@mkdir -p "$(@D)"
	@sed -e 's|<sys_size>|$(SYSTEM_SIZE)|g' \
         -e 's|<dat1>|0.001/bloom.dat|g' \
         -e 's|<dat2>|0.01/bloom.dat|g' \
         -e 's|<dat3>|0.1/bloom.dat|g' \
         -e 's|<title1>|bloom_{P_{1e}0.001}|g' \
         -e 's|<title2>|bloom_{P_{1e}0.01}|g' \
         -e 's|<title3>|bloom_{P_{1e}0.1}|g' \
         -e 's|<morecommands>|absoluteRedundancy=1;stepSize=$(shell expr $* / 5);|g' < $< > $@

bloom_0-10-all/%/bloom.dat: bloom_0-10-all/Makefile
	@echo " executing bloom_0-10 $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a bloom -n "$(SYSTEM_SIZE)" -c "$*,2" -g eval_fprob.gp -t "bloom_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

bloom_0-100-all/%/bloom.dat: bloom_0-100-all/Makefile
	@echo " executing bloom_0-100 $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a bloom -n "$(SYSTEM_SIZE)" -c "$*,20" -g eval_fprob.gp -t "bloom_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 20 > "$(@D)/eval.log"

bloom-all: bloom_0-10-all/0.1/bloom.dat bloom_0-10-all/0.01/bloom.dat bloom_0-10-all/0.001/bloom.dat bloom_0-100-all/0.1/bloom.dat bloom_0-100-all/0.01/bloom.dat bloom_0-100-all/0.001/bloom.dat
	

# ddist+fdist analysis (different data and failure distributions) for different bloom recon parameters
bloom_ddists_fdists-all/%/bloom_ddists_fdists.dat: bloom_ddists_fdists-all/Makefile
	@echo " executing bloom_ddists_fdists $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a bloom_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$*,2" -g eval_fprob_dists.gp -t "bloom_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

bloom_dists-all: bloom_ddists_fdists-all/0.01/bloom_ddists_fdists.dat
	

# scalability analysis (increasing data size) for different bloom recon parameters
bloom_scale-all/%/bloom_scale.dat: bloom_scale-all/Makefile
	@echo " executing bloom_scale $* (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a bloom_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$*" -g eval_data_inc.gp -t "bloom_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

bloom_scale-all: bloom_scale-all/0.01/bloom_scale.dat
	

bloom: bloom-all bloom_dists-all bloom_scale-all
	

# fprob analysis (different delta) for different merkle recon parameters
merkle_0-10-all/%/merkle.dat: merkle_0-10-all/Makefile
	@echo " executing merkle_0-10 $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; ./rr_eval.sh -a merkle -n "$(SYSTEM_SIZE)" -c "$$V,$$B,$$P,2" -g eval_fprob.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

merkle_0-100-all/%/merkle.dat: merkle_0-100-all/Makefile
	@echo " executing merkle_0-100 $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; ./rr_eval.sh -a merkle -n "$(SYSTEM_SIZE)" -c "$$V,$$B,$$P,20" -g eval_fprob.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) -s 20 > "$(@D)/eval.log"

merkle-all_p0.1: merkle_0-10-all/v2_b1_p0.1/merkle.dat merkle_0-10-all/v2_b3_p0.1/merkle.dat merkle_0-10-all/v2_b15_p0.1/merkle.dat merkle_0-10-all/v4_b1_p0.1/merkle.dat merkle_0-10-all/v4_b3_p0.1/merkle.dat merkle_0-10-all/v4_b15_p0.1/merkle.dat merkle_0-10-all/v16_b1_p0.1/merkle.dat merkle_0-10-all/v16_b3_p0.1/merkle.dat merkle_0-10-all/v16_b15_p0.1/merkle.dat
	

merkle-all_p0.01: merkle_0-10-all/v2_b1_p0.01/merkle.dat merkle_0-10-all/v2_b3_p0.01/merkle.dat merkle_0-10-all/v2_b15_p0.01/merkle.dat merkle_0-10-all/v4_b1_p0.01/merkle.dat merkle_0-10-all/v4_b3_p0.01/merkle.dat merkle_0-10-all/v4_b15_p0.01/merkle.dat merkle_0-10-all/v16_b1_p0.01/merkle.dat merkle_0-10-all/v16_b3_p0.01/merkle.dat merkle_0-10-all/v16_b15_p0.01/merkle.dat merkle_0-100-all/v2_b1_p0.01/merkle.dat merkle_0-100-all/v4_b1_p0.01/merkle.dat merkle_0-100-all/v4_b3_p0.01/merkle.dat merkle_0-100-all/v4_b15_p0.01/merkle.dat merkle_0-100-all/v16_b1_p0.01/merkle.dat
	

merkle-all_p0.001: merkle_0-10-all/v2_b1_p0.001/merkle.dat merkle_0-10-all/v2_b3_p0.001/merkle.dat merkle_0-10-all/v2_b15_p0.001/merkle.dat merkle_0-10-all/v4_b1_p0.001/merkle.dat merkle_0-10-all/v4_b3_p0.001/merkle.dat merkle_0-10-all/v4_b15_p0.001/merkle.dat merkle_0-10-all/v16_b1_p0.001/merkle.dat merkle_0-10-all/v16_b3_p0.001/merkle.dat merkle_0-10-all/v16_b15_p0.001/merkle.dat
	

merkle-all: merkle-all_p0.1 merkle-all_p0.01 merkle-all_p0.001
	

# ddist+fdist analysis (different data and failure distributions) for different merkle recon parameters
merkle_ddists_fdists-all/%/merkle_ddists_fdists.dat: merkle_ddists_fdists-all/Makefile
	@echo " executing merkle_ddists_fdists $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; ./rr_eval.sh -a merkle_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$$V,$$B,$$P,2" -g eval_fprob_dists.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

merkle_dists-all: merkle_ddists_fdists-all/v2_b1_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all/v4_b1_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all/v4_b3_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all/v4_b15_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all/v16_b1_p0.01/merkle_ddists_fdists.dat
	

# scalability analysis (increasing data size) for different merkle recon parameters
merkle_scale-all/%/merkle_scale.dat: merkle_scale-all/Makefile
	@echo " executing merkle_scale $* (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; ./rr_eval.sh -a merkle_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$$V,$$B,$$P" -g eval_data_inc.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

merkle_scale-all: merkle_scale-all/v2_b1_p0.01/merkle_scale.dat merkle_scale-all/v4_b1_p0.01/merkle_scale.dat merkle_scale-all/v4_b3_p0.01/merkle_scale.dat merkle_scale-all/v4_b15_p0.01/merkle_scale.dat merkle_scale-all/v16_b1_p0.01/merkle_scale.dat
	

merkle_effective_prob/%/merkle_custom.dat: merkle_effective_prob/Makefile
	@echo " executing merkle_eff_prob $*"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; STR_D=$${STR:`expr index "$$STR" d`} ; STR_N=$${STR:`expr index "$$STR" n`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; D=`expr match "$$STR_D" '\([0-9][.0-9]*\)'` ; START=`expr match "$$STR_N" '\([0-9][0-9]*\)-[0-9][0-9]*,[0-9][0-9]*'` ; END=`expr match "$$STR_N" '[0-9][0-9]*-\([0-9][0-9]*\),[0-9][0-9]*'` ; STEPSIZE=`expr match "$$STR_N" '[0-9][0-9]*-[0-9][0-9]*,\([0-9][0-9]*\)'`; STEPS=`expr '(' $$END - $$START ')' / $$STEPSIZE` ; ./rr_eval.sh -a merkle_custom -n "$$START" -c "$$V,$$B,$$P,$$STEPSIZE,$$STEPS,[update],$$D" -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) -s "$$STEPSIZE" > "$(@D)/eval.log"

merkle: merkle-all merkle_dists-all merkle_scale-all
	
