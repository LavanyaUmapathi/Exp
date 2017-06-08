#!/bin/sh
#
# Cron job: load the latest MAAS account data
#


if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. /etc/acumen/config.sh

PREFIX=maas-account-load

######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

lockfile="/tmp/$PREFIX.lock"
if [ -z "$flock_mal" ] ; then
  lockopts="-n $lockfile"
  exec env flock_mal=1 flock $lockopts $0 "$@"
fi

$ADMIN/maas-etl/maas-account-load.sh >> $LOG 2>&1
status=$?
log-to-flume < $LOG
cat $LOG
if test $status != 0; then
  alert-to-pager-duty -l $LOG -s $PREFIX < $LOG
fi

rm -f $lockfile
