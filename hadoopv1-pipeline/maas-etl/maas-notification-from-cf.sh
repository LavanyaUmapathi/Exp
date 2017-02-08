#!/bin/bash
#
# Download MAAS notification data from cloud files day by day
#
# USAGE: stacktach-load-date.sh [OPTIONS] YYYY-MM-DD [YYYY-MM-DD]
# -n/--dryrun : show but do not execute
#
# If two dates are given they describe an inclusive range
#

program=`basename $0`

# SWIFT config
API_V1=https://identity.api.rackspacecloud.com/v1.0
API_V2=https://identity.api.rackspacecloud.com/v2.0

. /etc/acumen/swiftly.sh

# python-swiftclient program configuration
export ST_AUTH=$API_V1
export ST_USER=$SWIFTLY_AUTH_USER
export ST_KEY=$SWIFTLY_AUTH_KEY

SECONDS_IN_DAY=86400

# python-swiftclient
SWIFT_ARGS="--quiet --retries=10"
SWIFT_DOWNLOAD_ARGS="--skip-identical --object-threads 10"

# Hadoop fs command
#HADOOP_FS="hadoop fs"
HADOOP_FS="hdfs dfs"

# File system

# Needs lots of free space. 250G/day and growing
# /tmp is usually far too small
tmpdir="/home/maas/tmp_notifications"

out_file=$(mktemp)

# HDFS
HDFS_MAAS_NOTIFICATION_DIR="/user/maas/notification-data"


######################################################################
# Nothing below here should need changing

usage() {
  echo "Usage: $program [OPTIONS] START-DATE [END-DATE]" 1>&2
  echo "  dates are in format YYYY-MM-DD" 1>&2
  echo "OPTIONS:" 1>&2
  echo "  -h             Show this help message" 1>&2
  echo "  -n / --dryrun  Show but do not execute commands" 1>&2
}

dryrun=0
while getopts "hn" o ; do
  case "${o}" in
    h)
     usage
     exit 0
     ;;
    n)
     dryrun=1
     ;;
    \?)
      echo "$program: ERROR: Unknown option -$OPTARG" 1>&2
      echo "$program: Use $program -h for usage" 1>&2
      exit 1
      ;;
   esac
done

shift $((OPTIND-1))

if [ $# -lt 1 -o $# -gt 2 ]; then
  echo "$program: ERROR: expected 1 or 2 arguments" 1>&2
  usage
  exit 1
fi

start_date=$1
if [ $# -gt 1 ] ; then
  end_date=$2
else
  end_date=$start_date
fi

if test $start_date '>' $end_date; then
  echo "$program: ERROR: Start date $start_date is after end date $end_date" 1>&2;
  exit 1
fi

if test $dryrun = 1; then
  echo "$program: Running in DRYRUN mode - no executing" 1>&2
fi


trap "rm -f $out_file" 0 9 15


today=`date -u +%Y-%m-%d`

echo "$program: Transferring files for $start_date to $end_date inclusive" 1>&2

if test $start_date = $today -o $start_date '<' $today; then
  if test $today = $end_date -o $today '<' $end_date; then
    echo "$program: ERROR: today is in range $start_date to $end_date - ending" 1>&2
    exit 1
  fi
fi

start_date_nom=`echo $start_date | tr -d '-'`
end_date_nom=`echo $end_date | tr -d '-'`


mkdir -p $tmpdir
cd $tmpdir

now=`date +%Y-%m-%dT%H:%M:%S`
echo "$program: Starting downloading dates $start_date to $end_date inclusive" 1>&2

date=$start_date
while test $date '<' $end_date -o $date '=' $end_date; do
    # HDFS output directory
    hdfs_date_dir="$HDFS_MAAS_NOTIFICATION_DIR/$date"

    now=`date +%Y-%m-%dT%H:%M:%S`
    echo "$program: $now Downloading from CF date $date" 1>&2
    date_nom=`echo $date | tr -d '-'`

    cmd="swift $SWIFT_ARGS download $SWIFT_DOWNLOAD_ARGS --prefix $date_nom notification-data"
    
    if test $dryrun = 1; then
        echo "$program: DRYRUN would run $cmd"
    else
        $cmd
        status=$?
        if test $status != 0; then
          echo "$program: FAILED $cmd with result $status" 1>&2
          #exit $status
        fi
    fi

    dus=`du -sh | awk '{print $1}' `
    now=`date +%Y-%m-%dT%H:%M:%S`
    echo "$program: $now Downloaded $dus data" 1>&2

    nfiles=$(ls -1 | wc -l)

    if test $nfiles -gt 0; then
        cmd="$HADOOP_FS -rm -r -skipTrash $hdfs_date_dir"
        if test $dryrun = 1; then
          echo "$program: DRYRUN would run $cmd"
        else
          $cmd
          # Ignore failure here
        fi

        cmd="$HADOOP_FS -mkdir $hdfs_date_dir"
        if test $dryrun = 1; then
          echo "$program: DRYRUN would run $cmd"
        else
          $cmd
          status=$?
          if test $status != 0; then
            echo "$program: FAILED $cmd with result $status" 1>&2
            exit $status
          fi
        fi

        now=`date +%Y-%m-%dT%H:%M:%S`
        echo "$program: $now Uploading to HDFS date $date" 1>&2
        cmd="$HADOOP_FS -moveFromLocal * /user/maas/notification-data/$date/"
        if test $dryrun = 1; then
          echo "$program: DRYRUN would run $cmd"
        else
          $cmd
          status=$?
          if test $status != 0; then
            echo "$program: FAILED $cmd with result $status" 1>&2
            exit $status
          fi
        fi

        now=`date +%Y-%m-%dT%H:%M:%S`
        echo "$program: $now Resulting HDFS dir size" 1>&2
        cmd="$HADOOP_FS -du -s -h /user/maas/notification-data/$date/"
        if test $dryrun = 1; then
          echo "$program: DRYRUN would run $cmd"
        else
          $cmd
          status=$?
          if test $status != 0; then
            echo "$program: FAILED $cmd with result $status" 1>&2
            exit $status
          fi
        fi
        cmd="$HADOOP_FS -mv /user/maas/notification-data/$date/* /user/maas/notification-data"
        if test $dryrun = 1; then
          echo "$program: DRYRUN would run $cmd"
        else
          $cmd
          status=$?
          if test $status != 0; then
            echo "$program: FAILED $cmd with result $status" 1>&2
            exit $status
          fi
        fi
        cmd="$HADOOP_FS -rm -r -skipTrash /user/maas/notification-data/$date"
        if test $dryrun = 1; then
          echo "$program: DRYRUN would run $cmd"
        else
          $cmd
          status=$?
          if test $status != 0; then
            echo "$program: FAILED $cmd with result $status" 1>&2
            exit $status
          fi
        fi
        
    fi

    # Advance date one day
    ts=`date -u -d $date +%s`
    ts=`expr $ts + $SECONDS_IN_DAY`
    date=`date -u -d @$ts +%Y-%m-%d`
done

now=`date +%Y-%m-%dT%H:%M:%S`
echo "$program: $now Done"

cd /
rm -rf $tmpdir