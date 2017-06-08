#!/bin/sh
#
# Copy MAAS account data from CF to HDFS
#
# USAGE: maas-account-copy.sh [OPTIONS]
# -n/--dryrun : show but do not execute
#

if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. /etc/acumen/config.sh

program=`basename $0`

# cloud_files_to_hdfs_copy.py configuration parameters
MIN=1
MAX=5

# File system
MARKER_FILE="$MARKERS/maas-account-data"
CONFIG_FILE="$CONFIG/cm0warehouse-account.json"

######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

dryrun=0
for arg in "$@"; do
  case $arg in
    -n|--dryrun)
     dryrun=1
     shift
     ;;
    -*)
      echo "$program: Unknown option $arg" 1>&2
      exit 1
      ;;
    *)
      ;;
   esac
done


echo "$program: Running cloud_files_to_hdfs_copy.py to copy account config" 1>&2

cmd="$ACUMEN_DATA_ROOT/scripts/cloud_files_to_hdfs_copy.py --verbose --min $MIN --max $MAX -m $MARKER_FILE -o $HDFS_MAAS_HOME/account-data $CONFIG_FILE"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  echo "$program: running $cmd"
  $cmd
  status=$?
  if test $status != 0; then
    echo "$program: FAILED $cmd with result $status" 1>&2
    exit $status
  fi
fi

