#!/bin/bash
#
# Load the MAAS metrics data for a given DATE
#
# USAGE: maas-metrics-load-date.sh [OPTIONS] YYYY-MM-DD [YYYY-MM-DD]
# -n/--dryrun : show but do not execute
#
# If two dates are given they describe an inclusive range
#


if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. $ADMIN/cron/config.sh


program=`basename $0`

# File system
raw_data_file_list="/tmp/${program}-$$-metric-data-files.lst"
input_data_file_list="/tmp/${program}-$$-input-data-files.lst"
hql_script_file="/tmp/${program}-$$-script.hql"
db_files_list="/tmp/${program}-$$-db-files.lst"
hive_out_file="/tmp/${program}-$$-hive-out.log"
hive_err_file="/tmp/${program}-$$-hive-err.log"

# HDFS config
HDFS_WORKING_DIR="/tmp/${program}-$$"
HDFS_DATA_INPUT_DIR="$HDFS_MAAS_INPUT_DIR"

# Hive config
HIVE_DATABASE="$HIVE_MAAS_DATABASE"
HIVE_TABLE="$HIVE_MAAS_TABLE"
HIVE_PROD_TABLE="$HIVE_TABLE"
HIVE_STAGING1_TABLE="${HIVE_TABLE}_$$_stg1"
HIVE_STAGING2_TABLE="${HIVE_TABLE}_$$_stg2"
HIVE_PROD_TABLE_DIR="$HIVE_HDFS_ROOT/${HIVE_MAAS_DATABASE}.db/${HIVE_TABLE}"


######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

usage() { 
  echo "Usage: $program [OPTIONS] START-DATE [END-DATE]" 1>&2
  echo "  dates are in format YYYY-MM-DD" 1>&2
  echo "OPTIONS:" 1>&2
  echo "  -h             Show this help message" 1>&2
  echo "  -e DAYS        Scan DAYS extra days for data" 1>&2
  echo "  -n / --dryrun  Show but do not execute commands" 1>&2
}


dryrun=0
extras=0
while getopts ":e:hn" o ; do
  case "${o}" in
    e)
     extras=$OPTARG
     ;;
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

if test $extras -ne 0; then
  echo "$program: Adding $extras extra days to scan" 1>&2
fi


trap "rm -f $raw_data_file_list $input_data_file_list $hql_script_file $db_files_list $hive_out_file $hive_err_file" 0 9 15


# Input: $dt date YYYY-MM-DD
# Return: $ts holds unix timestamp of noon UTC on that day
dt_to_ts () {
  dt=$1
  dtime="$dt 12:00:00 UTC"
  if test `uname` = "Darwin"; then
    ts=`date -u -j -f '%Y-%m-%d %H:%M:%S %Z' "$dtime" '+%s'`
  else
    ts=`date -u -d "$dtime" '+%s'`
  fi
  return 0
}

# Input: $ts unix timestamp
# Return: $dt holds date YYYY-MM-DD corresponding to unix timestamp $ts
ts_to_dt () {
  ts=$1
  if test `uname` = "Darwin"; then
    dt=`date -u -j -r $ts '+%Y-%m-%d'`
  else
    dt=`date -u -d @$ts '+%Y-%m-%d'`
  fi
  return 0
}

# Input: $dt date YYYY-MM-DD, $offset in days
# Return: $dt holds date offset by $N days (positive or negative)
dt_offset () {
  dt=$1
  offset=$2
  dt_to_ts $dt
  ts=`expr $ts + '(' $offset '*' $SECONDS_IN_DAY ')'`
  ts_to_dt $ts
}


# Get date before start of range to add a few more files
dt_offset $start_date -1
previous_date=$dt

# Extend end of range by $extras days
dt_offset $end_date $extras
last_date=$dt

today=`date -u +%Y-%m-%d`

echo "$program: Loading data $start_date to $end_date inclusive" 1>&2
echo "$program: Scanning days $previous_date to $last_date" 1>&2

if test $start_date = $today -o $start_date '<' $today; then
  if test $today = $end_date -o $today '<' $end_date; then
    echo "$program: ERROR: today is in range $start_date to $end_date - ending" 1>&2
    exit 1
  fi
fi


echo "$program: Looking for input files in $HDFS_DATA_INPUT_DIR:" 1>&2
echo "                 $previous_date: newest $MAAS_PREVIOUS_DAY_FILES_COUNT files" 1>&2
echo "  $start_date ... $end_date: all files" 1>&2
if test $extras -ne 0; then
  echo " >$end_date ... $last_date: all files (extra $extras days)" 1>&2
fi

previous_date_nom=`echo $previous_date | tr -d '-'`
start_date_nom=`echo $start_date | tr -d '-'`
last_date_nom=`echo $last_date | tr -d '-'`

# Output file format: <date-from-path> \t <full-hdfs-path> \t <file-name>
hadoop fs -ls $HDFS_DATA_INPUT_DIR/* | \
  awk '!/^Found/ { print $8 }' | \
  sed -e 's,^\(.*/\)\([0-9]*\)\(.*\),\2\t\1\2\3\t\2\3,' \
  > $raw_data_file_list

awk "{if(\$1 == \"${previous_date_nom}\") { print \$2 } }" < $raw_data_file_list | tail -n $MAAS_PREVIOUS_DAY_FILES_COUNT > $input_data_file_list
awk "{if(\$1 >= \"${start_date_nom}\" && \$1 <= \"${last_date_nom}\") { print \$2 } }" < $raw_data_file_list  >> $input_data_file_list

input_files_count=`wc -l < $input_data_file_list`

if test $input_files_count -eq 0; then
  echo "$program: No data available for date range $start_date ... $end_date" 1>&2
  exit 1
fi

# Calculate number of days
days=`awk "{print $1}" < $raw_data_file_list | sort -u | wc -l`
if test $days -eq 0; then
  days=1
fi

echo "$program: Found $input_files_count files for date range $start_date ... $end_date ($days days)" 1>&2
if test $dryrun = 1; then
  echo "$program: Would process these input files" 1>&2
  sed -e 's/^/    /' $input_data_file_list  1>&2
fi

# Delete working space
cmd="hadoop fs -rm -r $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  $cmd && exit 0
fi


reducers=`expr 50 \* $days`

# Run the Map-Reduce job to load JSON files from the
# data area and write it as files below $HDFS_WORKING_DIR
echo "$program: Running Map-Reduce job with $reducers reducers to load metrics" 1>&2

REDUCER_OPTS="-Dmapreduce.job.reduces=$reducers"

cmd="yarn jar $MAAS_ETL_JAR $MAAS_ETL_JAR_CLASS $COMMON_ETL_JAR_OPTS $REDUCER_OPTS -o $HDFS_WORKING_DIR -if $input_data_file_list -of text -et $MAAS_EVENT_TYPE"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  echo "$program: running $cmd"
  $cmd
  status=$?
  if test $status != 0; then
    echo "$program: FAILED Map-Reduce job $cmd with result $status" 1>&2
    exit $status
  fi
fi

file_glob="$HDFS_WORKING_DIR/${MAAS_EVENT_TYPE}*"

# Check the ETL job outputs
hadoop fs -ls $file_glob 2>/dev/null | sed -e 's/^/    /' > $db_files_list
db_files_list_count=`wc -l < $db_files_list`

if test $dryrun = 1; then
  :
else
  if test $db_files_list_count -eq 0; then
    echo "$program: ERROR: ETL job created no output files for date range $start_date ... $end_date" 1>&2
    hadoop fs -rm -r $HDFS_WORKING_DIR
    exit 1
  fi
fi

# Summarize the resulting files in the Hive DB
echo "$program: Processing loading $db_files_list_count files into $HIVE_TABLE" 1>&2
echo "     Source: $file_glob" 1>&2
  sed -e 's/^/    /' $db_files_list  1>&2
echo "  Staging 1: Hive table $HIVE_STAGING1_TABLE" 1>&2
echo "  Staging 2: Hive table $HIVE_STAGING2_TABLE" 1>&2
echo "       Dest: Hive table $HIVE_PROD_TABLE" 1>&2


# Build HQL to create staging table and load ETL output files into it
cat > $hql_script_file <<EOF
USE ${HIVE_DATABASE};

SET hive.execution.engine=mr;

DROP TABLE IF EXISTS ${HIVE_STAGING1_TABLE};

CREATE TABLE ${HIVE_STAGING1_TABLE} (
id STRING,
account_id STRING,
tenant_id STRING,
entity_id STRING,
check_id STRING,
dimension_key STRING,
target STRING,
check_type STRING,
monitoring_zone_id STRING,
collector_id STRING,
available boolean,
ts BIGINT,
month INT,
day INT,
year INT,
metrics map<STRING,STRUCT<type:INT,value:STRING,unit:STRING,unit_other:STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\\\'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n';

LOAD DATA INPATH '${file_glob}' INTO TABLE ${HIVE_STAGING1_TABLE};

EOF

echo "$program: Running load to staging hive HQL from $hql_script_file" 1>&2
cmd="hive $HIVE_OPTS -f $hql_script_file"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd :" 1>&2
  cat $hql_script_file
else
  echo "$program: running $cmd"
  $cmd 2>&1 | tee $hive_out_file
  status=$?
  sed -e 's/^/    /' $hive_out_file 1>&2
  if test $status != 0; then
    echo "$program: FAILED load to staging hive $cmd with result $status" 1>&2
    exit $status
  fi
fi


# Build HQL to create second staging table and load ETL output files into it
cat > $hql_script_file <<EOF
USE ${HIVE_DATABASE};

DROP TABLE IF EXISTS ${HIVE_STAGING2_TABLE};

CREATE TABLE ${HIVE_STAGING2_TABLE} (
id STRING,
account_id STRING,
tenant_id STRING,
entity_id STRING,
check_id STRING,
dimension_key STRING,
target STRING,
check_type STRING,
monitoring_zone_id STRING,
collector_id STRING,
available boolean,
ts BIGINT,
metrics map<STRING,STRUCT<type:INT,value:STRING,unit:STRING,unit_other:STRING>>,
dt STRING,
hr STRING
)
STORED AS SEQUENCEFILE
;

SET hive.exec.compress.output=true;
SET hive.execution.engine=mr;

SET mapreduce.job.reduce.slowstart.completedmaps=0.999;

SET io.seqfile.compression.type=BLOCK;

FROM ${HIVE_STAGING1_TABLE} stg
INSERT OVERWRITE TABLE ${HIVE_STAGING2_TABLE}
  SELECT
    stg.id,
    stg.account_id,
    stg.tenant_id,
    stg.entity_id,
    stg.check_id,
    stg.dimension_key,
    stg.target,
    stg.check_type,
    stg.monitoring_zone_id,
    stg.collector_id,
    stg.available,
    stg.ts,
    stg.metrics,
    PRINTF('%04d-%02d-%02d', stg.year, stg.month, stg.day) AS dt,
    HOUR(FROM_UNIXTIME(stg.ts)) AS hr
;

DROP TABLE ${HIVE_STAGING1_TABLE};

EOF

echo "$program: Running load to staging 2 hive HQL from $hql_script_file" 1>&2
cmd="hive $HIVE_OPTS -f $hql_script_file"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd :" 1>&2
  cat $hql_script_file
else
  echo "$program: running $cmd"
  $cmd 2>&1 | tee $hive_out_file
  status=$?
  sed -e 's/^/    /' $hive_out_file 1>&2
  if test $status != 0; then
    echo "$program: FAILED load to staging 2 hive $cmd with result $status" 1>&2
    exit $status
  fi
fi

# Build HQL to load production table from staging 2
cat > $hql_script_file <<EOF
USE ${HIVE_DATABASE};

SET hive.enforce.sorting=true;
SET hive.exec.compress.output=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.execution.engine=mr;
SET hive.mapred.reduce.tasks.speculative.execution=false;
SET hive.merge.mapredfiles=true;

SET mapreduce.input.fileinputformat.split.maxsize=521000000;
SET mapreduce.job.jvm.numtasks=-1;
SET mapreduce.job.reduce.slowstart.completedmaps=0.999;
SET mapreduce.output.fileoutputformat.compress.type=BLOCK;
SET mapreduce.reduce.input.limit=-1;
SET mapreduce.task.io.sort.factor=100;
SET mapreduce.task.io.sort.mb=256;

FROM ${HIVE_STAGING2_TABLE} stg
INSERT OVERWRITE TABLE ${HIVE_PROD_TABLE} PARTITION(dt, hr)
  SELECT
    stg.account_id,
    stg.tenant_id,
    stg.entity_id,
    stg.check_id,
    stg.target,
    stg.monitoring_zone_id,
    stg.collector_id,
    stg.available,
    stg.ts,
    stg.check_type,
    stg.metrics,
    stg.dt,
    stg.hr
    WHERE stg.dt >= "${start_date}" AND stg.dt <= "${end_date}"
DISTRIBUTE BY stg.dt, stg.hr
;

DROP TABLE ${HIVE_STAGING2_TABLE};
EOF

echo "$program: Running load to prod hive HQL from $hql_script_file" 1>&2
cmd="hive $HIVE_OPTS -f $hql_script_file"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd :" 1>&2
  cat $hql_script_file
else
  echo "$program: running hive $cmd"
  $cmd 2>&1 | tee $hive_out_file
  status=$?
  sed -e 's/^/    /' $hive_out_file 1>&2
  if test $status != 0; then
    echo "$program: FAILED load to prod hive $cmd with result $status" 1>&2
    exit $status
  fi
fi


cmd="hadoop fs -chmod -R 755 ${HIVE_PROD_TABLE_DIR}"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  $cmd
  status=$?
  if test $status != 0; then
    echo "$program: FAILED $cmd with result $status" 1>&2
    exit $status
  fi
fi


# Build HQL to count production table for date (validation)
cat > $hql_script_file <<EOF
USE ${HIVE_DATABASE};

SET hive.execution.engine=mr;

SET mapreduce.job.reduce.slowstart.completedmaps=0.999;

SELECT dt, COUNT(1) from ${HIVE_PROD_TABLE}
WHERE dt >="${start_date}" AND dt <= "${end_date}"
GROUP BY dt
ORDER BY dt ASC
;
EOF

echo "$program: Running validate hive HQL from $hql_script_file" 1>&2
cmd="hive $HIVE_OPTS -f $hql_script_file"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd :" 1>&2
  cat $hql_script_file
else
  echo "$program: running $cmd"
  $cmd > $hive_out_file 2> $hive_err_file
  status=$?
  if test $status != 0; then
    echo "Hive stdout:" 1>&2
    sed -e 's/^/    /' $hive_out_file 1>&2
    echo "Hive stderr:" 1>&2
    sed -e 's/^/    /' $hive_err_file 1>&2
    echo "$program: FAILED validation hive $cmd with result $status" 1>&2
    exit $status
  fi
  echo "$program: Validation hive returned:" 1>&2
  sed -e 's/^/    /' $hive_out_file 1>&2
fi


# Delete working dir
cmd="hadoop fs -rm -r $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  echo "$program: running $cmd"
  $cmd
  status=$?
  if test $status != 0; then
    echo "$program: FAILED $cmd with result $status" 1>&2
    exit $status
  fi
fi
