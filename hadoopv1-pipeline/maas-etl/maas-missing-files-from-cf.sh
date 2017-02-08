#!/bin/sh
#
# Download the requested MAAS files from Cloud Files to local disk
# and upload them to HDFS.  Will *DELETE* any existing HDFS files of
# the same name, to allow replacing empty or truncated files
#
# USAGE: maas-missing-files-from-cf.sh METRIC-FILE...
#

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH

. /etc/acumen/swiftly.sh

. /etc/acumen/swift.sh

program=`basename $0`

HDFS_LIST_FILE=`mktemp`
CF_LIST_FILE=`mktemp`

for metric_file in $*; do
  echo "$program: Fixing $metric_file" 1>&2

  hdfs_name="/user/maas/metric-data/$metric_file"

  swift_name=`echo $metric_file | sed -e 's/\./_/g' -e 's/_json_gz/.json.gz/'`

  echo "$program: Existing HDFS file" 1>&2
  hadoop fs -ls $hdfs_name | grep -v '^Found' > $HDFS_LIST_FILE
  cat $HDFS_LIST_FILE 1>&2
  hdfs_file_size=`awk '{print $5}' $HDFS_LIST_FILE`

  TMP_DIR=`mktemp -d`
  mkdir -p $TMP_DIR
  echo $TMP_DIR
  cd $TMP_DIR

  export HADOOP_USER_NAME=maas

  echo "$program: Getting $metric_file from Cloud Files" 1>&2
  swift download metric-data $swift_name
  mv $swift_name $metric_file
  echo "$program: Resulting file" 1>&2
  ls -l $metric_file > $CF_LIST_FILE
  cat $CF_LIST_FILE 1>&2
  cf_file_size=`awk '{ print $5}' $CF_LIST_FILE`

  if test $hdfs_file_size == $cf_file_size; then
      echo "$program: Skipping CF file same size as HDFS ($hdfs_file_size)" 1>&2
      rm -f $metric_file
  else
      echo "$program: Storing $metric_file on HDFS" 1>&2
      hadoop fs -rm $hdfs_name
      hadoop fs -copyFromLocal $metric_file $hdfs_name
      hadoop fs -chown maas:maas $hdfs_name
      hadoop fs -chmod 644 $hdfs_name
  fi

  cd /tmp
  rm -rf $TMP_DIR
done
