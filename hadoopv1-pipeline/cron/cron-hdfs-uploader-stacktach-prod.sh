#!/bin/sh
#
# Cron job: HDFS uplaoder

if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. $ADMIN/cron/config.sh

PREFIX=hdfs-uploader-stacktach-prod

######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

lockfile="/tmp/$PREFIX.lock"
if [ -z "$flock_hdfs_uploader_stacktach_prod" ] ; then
  lockopts="-n $lockfile"
  exec env flock_hdfs_uploader_stacktach_prod=1 flock $lockopts $0 "$@"
fi

hdfs-uploader.py -c $CONFIG/hdfs-uploader-stacktach.conf -s stacktach-v3-prod >> $LOG 2>&1

rm -f $lockfile
