#!/bin/sh
#
# Cron job: load the latest stacktach data into Hive
#

if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. $ADMIN/cron/config.sh

PREFIX=stacktach-load

######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

lockfile="/tmp/$PREFIX.lock"
if [ -z "$flock_stacktach_load" ] ; then
  lockopts="-n $lockfile"
  exec env flock_stacktach_load=1 flock $lockopts $0 "$@"
fi

$ADMIN/stacktach-etl/stacktach-load.sh >> $LOG 2>&1
status=$?
log-to-flume < $LOG
if test $status != 0; then
  alert-to-pager-duty -l $LOG -s $PREFIX < $LOG
fi

rm -f $lockfile
