LOG_FILE=$1
LOG_FILE_CLEAN=`echo $1 | sed 's/log/sum/g'`
cat $LOG_FILE | egrep 1\/s\|NPV | xargs -n 18 | sort -r -n -k 16 > $LOG_FILE_CLEAN
