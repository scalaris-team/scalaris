#!/bin/bash -e

scriptdir="$(dirname $0)"
scriptdir_abs=$(readlink -m "$scriptdir")

#--EXAMPLES
# 1) sh rr_eval.sh -a art -c dc1 -g eval_data_inc.gp
# 2) sh rr_eval.sh -a art -c fp1 -g eval_fprob.gp

#--------OPTIONS----------------
set -e  #exit after any error

#--------PARAMETERS-------------
# SYSTEM_SIZE will be wrong in scale simulations but there it is not used!
SYSTEM_SIZE=10000
EVAL_REPEATS=100
STEPSIZE=
PORT_DIFF=random
GPSCRIPT=
while getopts a:g:c:t:d:p:s:n:r: option ; do
  case "${option}" in
    a) ALGO=${OPTARG};;
    c) CONFIG=${OPTARG};;
    g) GPSCRIPT=${OPTARG};;
    t) TITLE=${OPTARG};;
    d) DIRECTORY=${OPTARG};;
    p) PORT_DIFF=${OPTARG};;
    s) STEPSIZE="stepSize=${OPTARG};";;
    n) SYSTEM_SIZE=${OPTARG};;
    r) EVAL_REPEATS=${OPTARG};;
  esac
done
DIRECTORY=${DIRECTORY:-${ALGO}}
if [ "${DIRECTORY:0:1}" != "/" ]; then
  # relative directory
  DIRECTORY="${scriptdir_abs}/${DIRECTORY}"
fi
mkdir -p "${DIRECTORY}"

# track git status (use sub-shell so we can change the directory)
$(
cd "${scriptdir}/../../"
echo "> git status --untracked-files=no -s -b" > "${DIRECTORY}/git.status"
git status --untracked-files=no -s -b >> "${DIRECTORY}/git.status"
echo "" >> "${DIRECTORY}/git.status"
echo "> git log -1" >> "${DIRECTORY}/git.status"
git log -1 >> "${DIRECTORY}/git.status"
)

if [ "${PORT_DIFF}" = "random" ] ; then
  PORT_DIFF=$((${RANDOM}%6000))
fi

#--------START------------------
echo "---$ALGO evaluation----"

#eval run
echo ">run evaluation - $CONFIG"
SCALARIS_PORT=$((14000+${PORT_DIFF}))
YAWS_PORT=$((8000+${PORT_DIFF}))
EVALCMD="rr_eval_admin:${ALGO}(\"${DIRECTORY}\", \"${ALGO}.dat\", ${SYSTEM_SIZE}, ${EVAL_REPEATS}, ${CONFIG}), halt(0)."

# set the following config options:
# {monitor_perf_interval, 0}. % deactivate micro-benchmarks
# {lb_active_use_gossip, false}. % deactivate gossip load modules (only needed for active load balancing)
# {gossip_load_number_of_buckets, 1}. % no overhead by scanning through the DB
# {gossip_load_additional_modules, []}. % no active load measurements, e.g. CPU or RAM
${scriptdir}/../../bin/scalarisctl -t first_nostart -m start -p ${SCALARIS_PORT} -y ${YAWS_PORT} -n "node$RANDOM" -l "${DIRECTORY}" -e "-noinput -scalaris mgmt_server \"{{127,0,0,1},${SCALARIS_PORT},mgmt_server}\" -scalaris known_hosts \"[{{127,0,0,1},${SCALARIS_PORT},service_per_vm}]\" -scalaris monitor_perf_interval \"0\" -scalaris lb_active_use_gossip \"false\" -scalaris gossip_load_number_of_buckets \"1\" -scalaris gossip_load_additional_modules \"[]\" -pa ../test -eval '${EVALCMD}'"

#gnuplot
if [ -n "${GPSCRIPT}" ]; then
  # GPSCRIPT_REL=$(realpath --relative-to="${DIRECTORY}" ${GPSCRIPT})
  # COLDEFS_REL=$(realpath --relative-to="${DIRECTORY}" coldefs_eval_point.gp)
  GPSCRIPT_REL=$(python -c "import os.path; print os.path.relpath('${GPSCRIPT}', '${DIRECTORY}')")
  COLDEFS_REL=$(python -c "import os.path; print os.path.relpath('coldefs_eval_point.gp', '${DIRECTORY}')")
#   if [ "${ALGO:0:5}" == "bloom" ] ; then
#     REGEN_ACC_IN_PERCENT=";regenAccInPercent=1"
#   fi
  if [ "${ALGO:0:7}" == "trivial" -o "${ALGO:0:5}" == "shash" -o "${ALGO:0:5}" == "bloom" -o "${ALGO:0:6}" == "merkle" ] ; then
    ABSOLUTE_REDUNDANCY=";absoluteRedundancy=1"
  fi
  cat > "${DIRECTORY}/Makefile" <<MAKEFILESTRING
MAKEFLAGS=-k \$MAKEFLAGS
GNUPLOT=gnuplot

all: .${GPSCRIPT}-done

.${GPSCRIPT}-done: ${ALGO}.dat ${GPSCRIPT_REL} ${COLDEFS_REL} Makefile
	echo ">plot using [${GPSCRIPT}]"
	\$(GNUPLOT) -e "colDefFile='${COLDEFS_REL}';systemSize=${SYSTEM_SIZE};srcFile1='\$<';srcFile1_title='${TITLE}'${REGEN_ACC_IN_PERCENT}${ABSOLUTE_REDUNDANCY};${STEPSIZE}destDir='./'" "${GPSCRIPT_REL}"
	touch \$@

clean:
	rm -f *.pdf
	rm -f .${GPSCRIPT}-done
MAKEFILESTRING
fi

cat > "${DIRECTORY}/.gitignore" <<MAKEFILESTRING
/*.pdf
/.*-done
MAKEFILESTRING

echo "FINISH"
exit 0
