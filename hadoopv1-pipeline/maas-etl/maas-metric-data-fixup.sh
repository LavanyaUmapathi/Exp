#!/bin/sh
#
# Look for any short MAAS (Rackspace Monitoring) metric data files
# for the previous day and if there are some, download them from
# cloud files
#
# USAGE: maas-metric-data-fixup.sh
#

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH

. /etc/acumen/swiftly.sh

program=`basename $0`

MARKERS=/data/pipeline/markers

# File system
TS_FILE=$MARKERS/maas-metrics-timestamp

# TS_FILE contains next date (timestamp) to process
ts=`cat $TS_FILE`

# PORTABILITY: On OSX (BSD?) -d @$ts would be -r $ts
raw_dt=`date -u -d @$ts +%Y-%m-%d`
dt=`echo $raw_dt | tr -d -`

# Files are short if less than 90M
SHORT=90000000
#SHORT=226000000

LST=`mktemp`
OUT=`mktemp`

hdfs_glob="/user/maas/metric-data/$dt"

hadoop fs -ls ${hdfs_glob}* > $LST

awk "{if (\$5 > 0 && \$5 < $SHORT) { print \$5,\$8}}" $LST > $OUT
nout=`wc -l < $OUT`

if test $nout != 0; then
  echo "Short files for $dt"
  sed -E -e 's,(/user/maas/metric-data/)(.*),\1\2 \2,' $OUT
  files=`sed -E -e 's,^.*(/user/maas/metric-data/)(.*),\2,' $OUT`
  sh /data/acumen-admin/maas-etl/maas-missing-files-from-cf.sh $files
fi

rm -f $LST $OUT
