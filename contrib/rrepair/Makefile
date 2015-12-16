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
	@mkdir -p "$*"
	@cp gitignore.tmpl2 $@

%/Makefile: Makefile.tmpl
	@mkdir -p "$*"
	@cp Makefile.tmpl $@

# fprob analysis (different delta) for different trivial recon parameters
trivial_single-all.aligned/%/trivial.dat: trivial_single-all.aligned/Makefile
	@echo " executing trivial_single $*, aligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a trivial -n "$(SYSTEM_SIZE)" -c "$*,true,2" -g eval_fprob.gp -t "trivial_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

trivial_single-all.unaligned/Makefile: Makefile.tmpl2 trivial_single-all.unaligned/.gitignore
	@mkdir -p "trivial_single-all.unaligned"
	@sed -e 's|<sys_size>|$(SYSTEM_SIZE)|g' \
         -e 's|<dat1>|0.001/trivial.dat|g' \
         -e 's|<dat2>|0.01/trivial.dat|g' \
         -e 's|<dat3>|0.1/trivial.dat|g' \
         -e 's|<title1>|trivial_{0.001}|g' \
         -e 's|<title2>|trivial_{0.01}|g' \
         -e 's|<title3>|trivial_{0.1}|g' \
         -e 's|<morecommands>|absoluteRedundancy=1;|g' < $< > $@

trivial_single-all.unaligned/%/trivial.dat: trivial_single-all.unaligned/Makefile
	@echo " executing trivial_single $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a trivial -n "$(SYSTEM_SIZE)" -c "$*,false,2" -g eval_fprob.gp -t "trivial_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

trivial_0-100-all.unaligned/%/trivial.dat: trivial_0-100-all.unaligned/Makefile
	@echo " executing trivial_0-100 $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a trivial -n "$(SYSTEM_SIZE)" -c "$*,false,20" -g eval_fprob.gp -t "trivial_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 20 > "$(@D)/eval.log"

trivial.aligned-all: trivial_single-all.aligned/0.01/trivial.dat
	

trivial.unaligned-all: trivial_single-all.unaligned/0.1/trivial.dat trivial_single-all.unaligned/0.01/trivial.dat trivial_single-all.unaligned/0.001/trivial.dat trivial_0-100-all.unaligned/0.01/trivial.dat
	

# ddist+fdist analysis (different data and failure distributions) for different trivial recon parameters
trivial_ddists_fdists-all.unaligned/%/trivial_ddists_fdists.dat: trivial_ddists_fdists-all.unaligned/Makefile
	@echo " executing trivial_ddists_fdists $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a trivial_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$*,false,2" -g eval_fprob_dists.gp -t "trivial_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

trivial_dists.unaligned-all: trivial_ddists_fdists-all.unaligned/0.01/trivial_ddists_fdists.dat
	

# scalability analysis (increasing data size) for different trivial recon parameters
trivial_scale-all.unaligned/%/trivial_scale.dat: trivial_scale-all.unaligned/Makefile
	@echo " executing trivial_scale $*, unaligned (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a trivial_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$*,false" -g eval_data_inc.gp -t "trivial_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

trivial_scale.unaligned-all: trivial_scale-all.unaligned/0.01/trivial_scale.dat
	

trivial.unaligned: trivial.unaligned-all trivial_dists.unaligned-all trivial_scale.unaligned-all
	

trivial: trivial.unaligned

# fprob analysis (different delta) for different shash recon parameters
shash_single-all.aligned/%/shash.dat: shash_single-all.aligned/Makefile
	@echo " executing shash_single $*, aligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a shash -n "$(SYSTEM_SIZE)" -c "$*,true,2" -g eval_fprob.gp -t "shash_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

shash_single-all.unaligned/Makefile: Makefile.tmpl2 shash_single-all.unaligned/.gitignore
	@mkdir -p "shash_single-all.unaligned"
	@sed -e 's|<sys_size>|$(SYSTEM_SIZE)|g' \
         -e 's|<dat1>|0.001/shash.dat|g' \
         -e 's|<dat2>|0.01/shash.dat|g' \
         -e 's|<dat3>|0.1/shash.dat|g' \
         -e 's|<title1>|shash_{0.001}|g' \
         -e 's|<title2>|shash_{0.01}|g' \
         -e 's|<title3>|shash_{0.1}|g' \
         -e 's|<morecommands>|absoluteRedundancy=1;|g' < $< > $@

shash_single-all.unaligned/%/shash.dat: shash_single-all.unaligned/Makefile
	@echo " executing shash_single $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a shash -n "$(SYSTEM_SIZE)" -c "$*,false,2" -g eval_fprob.gp -t "shash_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

shash_0-100-all.unaligned/%/shash.dat: shash_0-100-all.unaligned/Makefile
	@echo " executing shash_0-100 $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a shash -n "$(SYSTEM_SIZE)" -c "$*,false,20" -g eval_fprob.gp -t "shash_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 20 > "$(@D)/eval.log"

shash.aligned-all: shash_single-all.aligned/0.01/shash.dat
	

shash.unaligned-all: shash_single-all.unaligned/0.1/shash.dat shash_single-all.unaligned/0.01/shash.dat shash_single-all.unaligned/0.001/shash.dat shash_0-100-all.unaligned/0.01/shash.dat
	

# ddist+fdist analysis (different data and failure distributions) for different shash recon parameters
shash_ddists_fdists-all.unaligned/%/shash_ddists_fdists.dat: shash_ddists_fdists-all.unaligned/Makefile
	@echo " executing shash_ddists_fdists $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a shash_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$*,false,2" -g eval_fprob_dists.gp -t "shash_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

shash_dists.unaligned-all: shash_ddists_fdists-all.unaligned/0.01/shash_ddists_fdists.dat
	

# scalability analysis (increasing data size) for different shash recon parameters
shash_scale-all.unaligned/%/shash_scale.dat: shash_scale-all.unaligned/Makefile
	@echo " executing shash_scale $*, unaligned (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a shash_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$*,false" -g eval_data_inc.gp -t "shash_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

shash_scale.unaligned-all: shash_scale-all.unaligned/0.01/shash_scale.dat
	

shash.unaligned: shash.unaligned-all shash_dists.unaligned-all shash_scale.unaligned-all
	

shash: shash.unaligned
	

# fprob analysis (different delta) for different bloom recon parameters
bloom_single-all.aligned/%/bloom.dat: bloom_single-all.aligned/Makefile
	@echo " executing bloom_single $*, aligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a bloom -n "$(SYSTEM_SIZE)" -c "$*,true,2" -g eval_fprob.gp -t "bloom_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

bloom_single-all.unaligned/Makefile: Makefile.tmpl2 bloom_single-all.unaligned/.gitignore
	@mkdir -p "bloom_single-all.unaligned"
	@sed -e 's|<sys_size>|$(SYSTEM_SIZE)|g' \
         -e 's|<dat1>|0.001/bloom.dat|g' \
         -e 's|<dat2>|0.01/bloom.dat|g' \
         -e 's|<dat3>|0.1/bloom.dat|g' \
         -e 's|<title1>|bloom_{0.001}|g' \
         -e 's|<title2>|bloom_{0.01}|g' \
         -e 's|<title3>|bloom_{0.1}|g' \
         -e 's|<morecommands>|regenAccInPercent=1;absoluteRedundancy=1;|g' < $< > $@

bloom_single-all.unaligned/%/bloom.dat: bloom_single-all.unaligned/Makefile
	@echo " executing bloom_single $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a bloom -n "$(SYSTEM_SIZE)" -c "$*,false,2" -g eval_fprob.gp -t "bloom_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

bloom_0-100-all.unaligned/%/bloom.dat: bloom_0-100-all.unaligned/Makefile
	@echo " executing bloom_0-100 $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a bloom -n "$(SYSTEM_SIZE)" -c "$*,false,20" -g eval_fprob.gp -t "bloom_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 20 > "$(@D)/eval.log"

bloom.aligned-all: bloom_single-all.aligned/0.01/bloom.dat
	

bloom.unaligned-all: bloom_single-all.unaligned/0.1/bloom.dat bloom_single-all.unaligned/0.01/bloom.dat bloom_single-all.unaligned/0.001/bloom.dat bloom_0-100-all.unaligned/0.01/bloom.dat
	

# ddist+fdist analysis (different data and failure distributions) for different bloom recon parameters
bloom_ddists_fdists-all.unaligned/%/bloom_ddists_fdists.dat: bloom_ddists_fdists-all.unaligned/Makefile
	@echo " executing bloom_ddists_fdists $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a bloom_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$*,false,2" -g eval_fprob_dists.gp -t "bloom_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

bloom_dists.unaligned-all: bloom_ddists_fdists-all.unaligned/0.01/bloom_ddists_fdists.dat
	

# scalability analysis (increasing data size) for different bloom recon parameters
bloom_scale-all.unaligned/%/bloom_scale.dat: bloom_scale-all.unaligned/Makefile
	@echo " executing bloom_scale $*, unaligned (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$(@D)"
	+./rr_eval.sh -a bloom_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$*,false" -g eval_data_inc.gp -t "bloom_{P_{1e}$*}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

bloom_scale.unaligned-all: bloom_scale-all.unaligned/0.01/bloom_scale.dat
	

bloom.unaligned: bloom.unaligned-all bloom_dists.unaligned-all bloom_scale.unaligned-all
	

bloom: bloom.unaligned
	

# fprob analysis (different delta) for different merkle recon parameters
merkle_single-all.aligned/%/merkle.dat: merkle_single-all.aligned/Makefile
	@echo " executing merkle_single $*, aligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; ./rr_eval.sh -a merkle -n "$(SYSTEM_SIZE)" -c "$$V,$$B,$$P,true,2" -g eval_fprob.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

merkle_single-all.unaligned/%/merkle.dat: merkle_single-all.unaligned/Makefile
	@echo " executing merkle_single $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; ./rr_eval.sh -a merkle -n "$(SYSTEM_SIZE)" -c "$$V,$$B,$$P,false,2" -g eval_fprob.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) -s 2 > "$(@D)/eval.log"

merkle_0-100-all.unaligned/%/merkle.dat: merkle_0-100-all.unaligned/Makefile
	@echo " executing merkle_0-100 $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; ./rr_eval.sh -a merkle -n "$(SYSTEM_SIZE)" -c "$$V,$$B,$$P,false,20" -g eval_fprob.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) -s 20 > "$(@D)/eval.log"

merkle.aligned-all: merkle_single-all.aligned/v2_b1_p0.01/merkle.dat merkle_single-all.aligned/v2_b3_p0.01/merkle.dat merkle_single-all.aligned/v2_b15_p0.01/merkle.dat merkle_single-all.aligned/v4_b1_p0.01/merkle.dat merkle_single-all.aligned/v4_b3_p0.01/merkle.dat merkle_single-all.aligned/v4_b15_p0.01/merkle.dat merkle_single-all.aligned/v16_b1_p0.01/merkle.dat merkle_single-all.aligned/v16_b3_p0.01/merkle.dat merkle_single-all.aligned/v16_b15_p0.01/merkle.dat
	

merkle.unaligned-all_p0.1: merkle_single-all.unaligned/v2_b1_p0.1/merkle.dat merkle_single-all.unaligned/v2_b3_p0.1/merkle.dat merkle_single-all.unaligned/v2_b15_p0.1/merkle.dat merkle_single-all.unaligned/v4_b1_p0.1/merkle.dat merkle_single-all.unaligned/v4_b3_p0.1/merkle.dat merkle_single-all.unaligned/v4_b15_p0.1/merkle.dat merkle_single-all.unaligned/v16_b1_p0.1/merkle.dat merkle_single-all.unaligned/v16_b3_p0.1/merkle.dat merkle_single-all.unaligned/v16_b15_p0.1/merkle.dat
	

merkle.unaligned-all_p0.01: merkle_single-all.unaligned/v2_b1_p0.01/merkle.dat merkle_single-all.unaligned/v2_b3_p0.01/merkle.dat merkle_single-all.unaligned/v2_b15_p0.01/merkle.dat merkle_single-all.unaligned/v4_b1_p0.01/merkle.dat merkle_single-all.unaligned/v4_b3_p0.01/merkle.dat merkle_single-all.unaligned/v4_b15_p0.01/merkle.dat merkle_single-all.unaligned/v16_b1_p0.01/merkle.dat merkle_single-all.unaligned/v16_b3_p0.01/merkle.dat merkle_single-all.unaligned/v16_b15_p0.01/merkle.dat merkle_0-100-all.unaligned/v2_b1_p0.01/merkle.dat merkle_0-100-all.unaligned/v4_b1_p0.01/merkle.dat merkle_0-100-all.unaligned/v4_b3_p0.01/merkle.dat merkle_0-100-all.unaligned/v4_b15_p0.01/merkle.dat merkle_0-100-all.unaligned/v16_b1_p0.01/merkle.dat
	

merkle.unaligned-all_p0.001: merkle_single-all.unaligned/v2_b1_p0.001/merkle.dat merkle_single-all.unaligned/v2_b3_p0.001/merkle.dat merkle_single-all.unaligned/v2_b15_p0.001/merkle.dat merkle_single-all.unaligned/v4_b1_p0.001/merkle.dat merkle_single-all.unaligned/v4_b3_p0.001/merkle.dat merkle_single-all.unaligned/v4_b15_p0.001/merkle.dat merkle_single-all.unaligned/v16_b1_p0.001/merkle.dat merkle_single-all.unaligned/v16_b3_p0.001/merkle.dat merkle_single-all.unaligned/v16_b15_p0.001/merkle.dat
	

merkle.unaligned-all: merkle.unaligned-all_p0.1 merkle.unaligned-all_p0.01 merkle.unaligned-all_p0.001
	

# ddist+fdist analysis (different data and failure distributions) for different merkle recon parameters
merkle_ddists_fdists-all.unaligned/%/merkle_ddists_fdists.dat: merkle_ddists_fdists-all.unaligned/Makefile
	@echo " executing merkle_ddists_fdists $*, unaligned (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; ./rr_eval.sh -a merkle_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$$V,$$B,$$P,false,2" -g eval_fprob_dists.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

merkle_dists.unaligned-all: merkle_ddists_fdists-all.unaligned/v2_b1_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all.unaligned/v4_b1_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all.unaligned/v4_b3_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all.unaligned/v4_b15_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all.unaligned/v16_b1_p0.01/merkle_ddists_fdists.dat
	

# scalability analysis (increasing data size) for different merkle recon parameters
merkle_scale-all.unaligned/%/merkle_scale.dat: merkle_scale-all.unaligned/Makefile
	@echo " executing merkle_scale $*, unaligned (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; ./rr_eval.sh -a merkle_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$$V,$$B,$$P,false" -g eval_data_inc.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

merkle_scale.unaligned-all: merkle_scale-all.unaligned/v2_b1_p0.01/merkle_scale.dat merkle_scale-all.unaligned/v4_b1_p0.01/merkle_scale.dat merkle_scale-all.unaligned/v4_b3_p0.01/merkle_scale.dat merkle_scale-all.unaligned/v4_b15_p0.01/merkle_scale.dat merkle_scale-all.unaligned/v16_b1_p0.01/merkle_scale.dat
	

merkle_effective_prob.unaligned/%/merkle_custom.dat: merkle_effective_prob.unaligned/Makefile
	@echo " executing merkle_eff_prob $*, unaligned"
	@mkdir -p "$(@D)"
	STR="$*" ; STR_V=$${STR:`expr index "$$STR" v`} ; STR_B=$${STR:`expr index "$$STR" b`} ; STR_P=$${STR:`expr index "$$STR" p`} ; STR_D=$${STR:`expr index "$$STR" d`} ; STR_N=$${STR:`expr index "$$STR" n`} ; V=`expr match "$$STR_V" '\([0-9][.0-9]*\)'` ; B=`expr match "$$STR_B" '\([0-9][.0-9]*\)'` ; P=`expr match "$$STR_P" '\([0-9][.0-9]*\)'` ; D=`expr match "$$STR_D" '\([0-9][.0-9]*\)'` ; START=`expr match "$$STR_N" '\([0-9][0-9]*\)-[0-9][0-9]*,[0-9][0-9]*'` ; END=`expr match "$$STR_N" '[0-9][0-9]*-\([0-9][0-9]*\),[0-9][0-9]*'` ; STEPSIZE=`expr match "$$STR_N" '[0-9][0-9]*-[0-9][0-9]*,\([0-9][0-9]*\)'`; STEPS=`expr '(' $$END - $$START ')' / $$STEPSIZE` ; ./rr_eval.sh -a merkle_custom -n "$$START" -c "$$V,$$B,$$P,false,$$STEPSIZE,$$STEPS,[update],$$D" -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) -s "$$STEPSIZE" > "$(@D)/eval.log"

merkle.unaligned: merkle.unaligned-all merkle_dists.unaligned-all merkle_scale.unaligned-all
	

merkle: merkle.unaligned
	
