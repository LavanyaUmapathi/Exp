#!/bin/bash
#
# Load the latest (by date) stacktach data
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
TS_FILE=$MARKERS/stacktach-timestamp

# TS_FILE contains next date (timestamp) to process
ts=`cat $TS_FILE`

# PORTABILITY: On OSX (BSD?) -d @$ts would be -r $ts
date=`date -u -d @$ts +%Y-%m-%d`

echo "$program: Loading stacktach for $date" 1>&2

$ADMIN/stacktach-etl/stacktach-load-date.sh $date
status=$?

if test $status -eq 0; then
  ts=`expr $ts + $SECONDS_IN_DAY`
  echo "$program: Updating last timestamp in $TS_FILE to $ts" 1>&2
  new="$TS_FILE.new"
  echo $ts > $new && mv $new $TS_FILE
fi

#Run stacktach requests ETL
echo "$program: Loading stacktach requests for $date" 1>&2

$ADMIN/stacktach-etl/stacktach-requests-load-date.sh $date
status=$?

if test $status -eq 0; then
  echo "$program: Successfully loaded stacktach requests" 1>&2
fi

