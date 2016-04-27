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
%-dir:
	$(MAKE) -C "$*"

%-dir-clean:
	$(MAKE) clean -C "$*"

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

EXP_DELTA_LIST=$(shell seq -s" " 0 0.1 1 ; seq -s" " 0 100)

define FPROB_MAKEFILE
$(1)_0-%_expD$(2)-all/Makefile: Makefile.tmpl2 $(1)_0-%_expD$(2)-all/.gitignore Makefile
	@mkdir -p "$$(@D)"
	@sed -e 's|<sys_size>|$(SYSTEM_SIZE)|g' \
         -e 's|<dat1>|0.001/$(1).dat|g' \
         -e 's|<dat2>|0.01/$(1).dat|g' \
         -e 's|<dat3>|0.1/$(1).dat|g' \
         -e 's|<title1>|$(1)_{P_{1e}0.001}|g' \
         -e 's|<title2>|$(1)_{P_{1e}0.01}|g' \
         -e 's|<title3>|$(1)_{P_{1e}0.1}|g' \
         -e 's|<morecommands>|absoluteRedundancy=1;stepSize=0$$(shell bc <<< "scale=2; $$*/5");|g' < $$< > $$@
endef
$(foreach ALG, trivial shash bloom, $(foreach EXP_DELTA, $(EXP_DELTA_LIST),\
$(eval $(call FPROB_MAKEFILE,$(ALG),$(EXP_DELTA),$(VARIANT)))\
))

# fprob analysis (different delta) for different recon parameters
define MAX_DELTA_TARGETS
.PRECIOUS: $(1)_0-$(2)_expD$(3)-all/Makefile
$(1)_0-$(2)_expD$(3)-all/%/$(1).dat: $(1)_0-$(2)_expD$(3)-all/Makefile
	@echo " executing $$(@D) (N=$(SYSTEM_SIZE))"
	@mkdir -p "$$(@D)"
	+STEP_SIZE=0$$(shell bc <<< "scale=2; $(2)/5") ; ./rr_eval.sh -a $(1) -n "$(SYSTEM_SIZE)" -c "$$*,$(3),$$$$STEP_SIZE" -g eval_fprob.gp -t "$(1)_{P_{1e}$$*}" -d "$$(@D)" -r $(EVAL_REPEATS) -s $$$$STEP_SIZE > "$$(@D)/eval.log"
endef
$(foreach ALG, trivial shash bloom, $(foreach MAX_DELTA, 1 10 100, $(foreach EXP_DELTA, $(EXP_DELTA_LIST),\
$(eval $(call MAX_DELTA_TARGETS,$(ALG),$(MAX_DELTA),$(EXP_DELTA)))\
)))

define OTHER_TARGETS
# ddist+fdist analysis (different data and failure distributions) for different recon parameters
.PRECIOUS: $(1)_ddists_fdists_expD$(2)-all/Makefile
$(1)_ddists_fdists_expD$(2)-all/%/$(1)_ddists_fdists.dat: $(1)_ddists_fdists_expD$(2)-all/Makefile
	@echo " executing $$(@D) (N=$(SYSTEM_SIZE))"
	@mkdir -p "$$(@D)"
	+STEP_SIZE=2 ; ./rr_eval.sh -a $(1)_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$$*,$(2),$$$$STEP_SIZE" -g eval_fprob_dists.gp -t "$(1)_{P_{1e}$$*}" -d "$$(@D)" -r $(EVAL_REPEATS) -s $$$$STEP_SIZE > "$$(@D)/eval.log"

# scalability analysis (increasing data size) for different recon parameters
.PRECIOUS: $(1)_scale_expD$(2)-all/Makefile
$(1)_scale_expD$(2)-all/Makefile: Makefile_scale.tmpl2 $(1)_scale_expD$(2)-all/.gitignore Makefile
	@mkdir -p "$$(@D)"
	@sed -e 's|<sys_size>|$(SYSTEM_SIZE_SCALE)|g' \
         -e 's|<dat1>|0.001/$(1)_scale.dat|g' \
         -e 's|<dat2>|0.01/$(1)_scale.dat|g' \
         -e 's|<dat3>|0.1/$(1)_scale.dat|g' \
         -e 's|<title1>|$(1)_{P_{1e}0.001}|g' \
         -e 's|<title2>|$(1)_{P_{1e}0.01}|g' \
         -e 's|<title3>|$(1)_{P_{1e}0.1}|g' \
         -e 's|<morecommands>|absoluteRedundancy=1;|g' < $$< > $$@

$(1)_scale_expD$(2)-all/%/$(1)_scale.dat: $(1)_scale_expD$(2)-all/Makefile
	@echo " executing $$(@D) (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$$(@D)"
	+./rr_eval.sh -a $(1)_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$$*,$(2)" -g eval_data_inc.gp -t "$(1)_{P_{1e}$$*}" -d "$$(@D)" -r $(EVAL_REPEATS) > "$$(@D)/eval.log"
endef
$(foreach ALG, trivial shash bloom, $(foreach EXP_DELTA, $(EXP_DELTA_LIST),\
$(eval $(call OTHER_TARGETS,$(ALG),$(EXP_DELTA)))\
))

# define summary targets for groups of the ones above:
define SUM_1
# fprob analysis (different delta)
$(1)_0-$(2)-all: $(1)_0-$(2)-all/0.1/$(1).dat $(1)_0-$(2)-all/0.01/$(1).dat $(1)_0-$(2)-all/0.001/$(1).dat
	
endef
$(foreach ALG, trivial shash bloom, $(foreach PAR, 1_expD0.4 10_expD4 100_expD40,\
$(eval $(call SUM_1,$(ALG),$(PAR)))\
))

define SUM_2
# fprob analysis (different delta)
$(1)-all: $(1)_0-1_expD0.4-all $(1)_0-10_expD4-all $(1)_0-100_expD40-all
	
endef
$(foreach ALG, trivial shash bloom, $(eval $(call SUM_2,$(ALG))))

define SUM_3
# ddist+fdist analysis (different data and failure distributions)
$(1)_dists-all: $(1)_ddists_fdists_expD4-all/0.01/$(1)_ddists_fdists.dat
	
# scalability analysis (increasing data size)
$(1)_scale-all: $(1)_scale_expD4-all/0.1/$(1)_scale.dat $(1)_scale_expD4-all/0.01/$(1)_scale.dat $(1)_scale_expD4-all/0.001/$(1)_scale.dat
	
$(1): $(1)-all $(1)_dists-all $(1)_scale-all
	
endef
$(foreach ALG, trivial shash bloom, $(eval $(call SUM_3,$(ALG))))

# v4_b3_p0.001
MERKLE_EXTRACT_V='v\([0-9][0-9]*\)_b[0-9][0-9]*_p[0-9][.0-9]*'
MERKLE_EXTRACT_B='v[0-9][0-9]*_b\([0-9][0-9]*\)_p[0-9][.0-9]*'
MERKLE_EXTRACT_P='v[0-9][0-9]*_b[0-9][0-9]*_p\([0-9][.0-9]*\)'

define MERKLE_FPROB_TARGETS
# fprob analysis (different delta) for different merkle recon parameters
.PRECIOUS: merkle_0-$(1)_expD$(2)-all/Makefile
merkle_0-$(1)_expD$(2)-all/%/merkle.dat: merkle_0-$(1)_expD$(2)-all/Makefile
	@echo " executing $$(@D) (N=$(SYSTEM_SIZE))"
	@mkdir -p "$$(@D)"
	+V=$$(shell expr match "$$*" $(MERKLE_EXTRACT_V)) ; B=$$(shell expr match "$$*" $(MERKLE_EXTRACT_B)) ; P=$$(shell expr match "$$*" $(MERKLE_EXTRACT_P)); STEP_SIZE=0$$(shell bc <<< "scale=2; $(1)/5") ; ./rr_eval.sh -a merkle -n "$(SYSTEM_SIZE)" -c "$$$$V,$$$$B,$$$$P,$(2),$$$$STEP_SIZE" -g eval_fprob.gp -t "merkle_{v$$$$V, b$$$$B, P_{1e}$$$$P}" -d "$$(@D)" -r $(EVAL_REPEATS) -s $$$$STEP_SIZE > "$$(@D)/eval.log"
endef
$(foreach MAX_DELTA, 1 10 100, $(foreach EXP_DELTA, $(EXP_DELTA_LIST),\
$(eval $(call MERKLE_FPROB_TARGETS,$(MAX_DELTA),$(EXP_DELTA)))\
))

merkle-all_p0.1: merkle_0-10-all/v2_b1_p0.1/merkle.dat merkle_0-10-all/v2_b3_p0.1/merkle.dat merkle_0-10-all/v2_b15_p0.1/merkle.dat merkle_0-10-all/v4_b1_p0.1/merkle.dat merkle_0-10-all/v4_b3_p0.1/merkle.dat merkle_0-10-all/v4_b15_p0.1/merkle.dat merkle_0-10-all/v16_b1_p0.1/merkle.dat merkle_0-10-all/v16_b3_p0.1/merkle.dat merkle_0-10-all/v16_b15_p0.1/merkle.dat
	

merkle-all_p0.01: merkle_0-10-all/v2_b1_p0.01/merkle.dat merkle_0-10-all/v2_b3_p0.01/merkle.dat merkle_0-10-all/v2_b15_p0.01/merkle.dat merkle_0-10-all/v4_b1_p0.01/merkle.dat merkle_0-10-all/v4_b3_p0.01/merkle.dat merkle_0-10-all/v4_b15_p0.01/merkle.dat merkle_0-10-all/v16_b1_p0.01/merkle.dat merkle_0-10-all/v16_b3_p0.01/merkle.dat merkle_0-10-all/v16_b15_p0.01/merkle.dat merkle_0-100-all/v2_b1_p0.01/merkle.dat merkle_0-100-all/v4_b1_p0.01/merkle.dat merkle_0-100-all/v4_b3_p0.01/merkle.dat merkle_0-100-all/v4_b15_p0.01/merkle.dat merkle_0-100-all/v16_b1_p0.01/merkle.dat
	

merkle-all_p0.001: merkle_0-10-all/v2_b1_p0.001/merkle.dat merkle_0-10-all/v2_b3_p0.001/merkle.dat merkle_0-10-all/v2_b15_p0.001/merkle.dat merkle_0-10-all/v4_b1_p0.001/merkle.dat merkle_0-10-all/v4_b3_p0.001/merkle.dat merkle_0-10-all/v4_b15_p0.001/merkle.dat merkle_0-10-all/v16_b1_p0.001/merkle.dat merkle_0-10-all/v16_b3_p0.001/merkle.dat merkle_0-10-all/v16_b15_p0.001/merkle.dat
	

merkle-all: merkle-all_p0.1 merkle-all_p0.01 merkle-all_p0.001
	

# ddist+fdist analysis (different data and failure distributions) for different merkle recon parameters
# TODO: integrate EXP_DELTA
merkle_ddists_fdists-all/%/merkle_ddists_fdists.dat: merkle_ddists_fdists-all/Makefile
	@echo " executing merkle_ddists_fdists $* (N=$(SYSTEM_SIZE))"
	@mkdir -p "$(@D)"
	V=$(shell expr match "$*" $(MERKLE_EXTRACT_V)) ; B=$(shell expr match "$*" $(MERKLE_EXTRACT_B)) ; P=$(shell expr match "$*" $(MERKLE_EXTRACT_P)) ; ./rr_eval.sh -a merkle_ddists_fdists -n "$(SYSTEM_SIZE)" -c "$$V,$$B,$$P,100,2" -g eval_fprob_dists.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

merkle_dists-all: merkle_ddists_fdists-all/v2_b1_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all/v4_b1_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all/v4_b3_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all/v4_b15_p0.01/merkle_ddists_fdists.dat merkle_ddists_fdists-all/v16_b1_p0.01/merkle_ddists_fdists.dat
	

# scalability analysis (increasing data size) for different merkle recon parameters
# TODO: integrate EXP_DELTA
merkle_scale-all/%/merkle_scale.dat: merkle_scale-all/Makefile
	@echo " executing merkle_scale $* (start at N=$(SYSTEM_SIZE_SCALE))"
	@mkdir -p "$(@D)"
	V=$(shell expr match "$*" $(MERKLE_EXTRACT_V)) ; B=$(shell expr match "$*" $(MERKLE_EXTRACT_B)) ; P=$(shell expr match "$*" $(MERKLE_EXTRACT_P)) ; ./rr_eval.sh -a merkle_scale -n "$(SYSTEM_SIZE_SCALE)" -c "$$V,$$B,$$P,100" -g eval_data_inc.gp -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) > "$(@D)/eval.log"

merkle_scale-all: merkle_scale-all/v2_b1_p0.01/merkle_scale.dat merkle_scale-all/v4_b1_p0.01/merkle_scale.dat merkle_scale-all/v4_b3_p0.01/merkle_scale.dat merkle_scale-all/v4_b15_p0.01/merkle_scale.dat merkle_scale-all/v16_b1_p0.01/merkle_scale.dat
	

# v4_b3_p0.001_d10_n32000-60000,4000
MERKLE_EFF_PROB_EXTRACT_V='v\([0-9][0-9]*\)_b[0-9][0-9]*_p[0-9][.0-9]*_d[0-9][0-9]*_n[0-9][0-9]*-[0-9][0-9]*,[0-9][0-9]*'
MERKLE_EFF_PROB_EXTRACT_B='v[0-9][0-9]*_b\([0-9][0-9]*\)_p[0-9][.0-9]*_d[0-9][0-9]*_n[0-9][0-9]*-[0-9][0-9]*,[0-9][0-9]*'
MERKLE_EFF_PROB_EXTRACT_P='v[0-9][0-9]*_b[0-9][0-9]*_p\([0-9][.0-9]*\)_d[0-9][0-9]*_n[0-9][0-9]*-[0-9][0-9]*,[0-9][0-9]*'
MERKLE_EFF_PROB_EXTRACT_D='v[0-9][0-9]*_b[0-9][0-9]*_p[0-9][.0-9]*_d\([0-9][0-9]*\)_n[0-9][0-9]*-[0-9][0-9]*,[0-9][0-9]*'
MERKLE_EFF_PROB_EXTRACT_START='v[0-9][0-9]*_b[0-9][0-9]*_p[0-9][.0-9]*_d[0-9][0-9]*_n\([0-9][0-9]*\)-[0-9][0-9]*,[0-9][0-9]*'
MERKLE_EFF_PROB_EXTRACT_END='v[0-9][0-9]*_b[0-9][0-9]*_p[0-9][.0-9]*_d[0-9][0-9]*_n[0-9][0-9]*-\([0-9][0-9]*\),[0-9][0-9]*'
MERKLE_EFF_PROB_EXTRACT_STEPSIZE='v[0-9][0-9]*_b[0-9][0-9]*_p[0-9][.0-9]*_d[0-9][0-9]*_n[0-9][0-9]*-[0-9][0-9]*,\([0-9][0-9]*\)'

merkle_effective_prob/%/merkle_custom.dat: merkle_effective_prob/Makefile
	@echo " executing merkle_eff_prob $*"
	@mkdir -p "$(@D)"
	V=$(shell expr match "$*" $(MERKLE_EFF_PROB_EXTRACT_V)) ; B=$(shell expr match "$*" $(MERKLE_EFF_PROB_EXTRACT_B)) ; P=$(shell expr match "$*" $(MERKLE_EFF_PROB_EXTRACT_P)) ; D=$(shell expr match "$*" $(MERKLE_EFF_PROB_EXTRACT_D)) ; START=$(shell expr match "$*" $(MERKLE_EFF_PROB_EXTRACT_START)) ; END=$(shell expr match "$*" $(MERKLE_EFF_PROB_EXTRACT_END)) ; STEPSIZE=$(shell expr match "$*" $(MERKLE_EFF_PROB_EXTRACT_STEPSIZE)) ; STEPS=`expr '(' $$END - $$START ')' / $$STEPSIZE` ; ./rr_eval.sh -a merkle_custom -n "$$START" -c "$$V,$$B,$$P,$$STEPSIZE,$$STEPS,[update],$$D" -t "merkle_{v$$V, b$$B, P_{1e}$$P}" -d "$(@D)" -r $(EVAL_REPEATS) -s "$$STEPSIZE" > "$(@D)/eval.log"

merkle: merkle-all merkle_dists-all merkle_scale-all
	
