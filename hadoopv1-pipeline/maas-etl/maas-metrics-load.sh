#!/bin/bash
#
# Load the latest (by date) MAAS metrics data
#


if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. $ADMIN/cron/config.sh


program=`basename $0`

######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

# File system
TS_FILE=$MARKERS/maas-metrics-timestamp

# TS_FILE contains next date (timestamp) to process
ts=`cat $TS_FILE`

# PORTABILITY: On OSX (BSD?) -d @$ts would be -r $ts
date=`date -u -d @$ts +%Y-%m-%d`

echo "$program: Loading metrics for $date" 1>&2

$ADMIN/maas-etl/maas-metrics-load-date.sh $date
status=$?

if test $status -eq 0; then
  ts=`expr $ts + $SECONDS_IN_DAY`
  echo "$program: Updating last timestamp in $TS_FILE to $ts" 1>&2
  new="$TS_FILE.new"
  echo $ts > $new && mv $new $TS_FILE
fi
