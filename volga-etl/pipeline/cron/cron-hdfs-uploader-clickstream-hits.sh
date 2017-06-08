#!/bin/sh
#
# Cron job: HDFS clickstream hits uplaoder

if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. $ADMIN/cron/config.sh

PREFIX=hdfs-uploader-clickstream-hits

######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

lockfile="/tmp/$PREFIX.lock"
if [ -z "$flock_hdfs_uploader_clickstream_hits" ] ; then
  lockopts="-n $lockfile"
  exec env flock_hdfs_uploader_clickstream_hits=1 flock $lockopts $0 "$@"
fi

hdfs-uploader.py -c $CONFIG/hdfs-uploader-clickstream.conf -s hits-data >> $LOG 2>&1

rm -f $lockfile
