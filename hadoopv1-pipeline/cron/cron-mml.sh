#!/bin/sh
#
# Cron job: load the latest MAAS metrics
#

if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. $ADMIN/cron/config.sh

PREFIX=maas-metrics-load

######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

lockfile="/tmp/$PREFIX.lock"
if [ -z "$flock_mml" ] ; then
  lockopts="-n $lockfile"
  exec env flock_mml=1 flock $lockopts $0 "$@"
fi

$ADMIN/maas-etl/maas-metric-data-fixup.sh >> $LOG 2>&1
$ADMIN/maas-etl/maas-metrics-load.sh >> $LOG 2>&1
status=$?
log-to-flume < $LOG
cat $LOG
if test $status != 0; then
  alert-to-pager-duty -l $LOG -s $PREFIX < $LOG
fi

rm -f $lockfile
