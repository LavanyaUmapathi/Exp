#!/bin/bash
#
# Load the stacktach requests data for a given DATE
#
# USAGE: stacktach-requests-load-date.sh [OPTIONS] YYYY-MM-DD [YYYY-MM-DD]
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
hql_script_file="/tmp/${program}-$$-script.hql"
hive_out_file="/tmp/${program}-$$-hive-out.log"
hive_err_file="/tmp/${program}-$$-hive-err.log"

# Hive config
#prod
HIVE_DATABASE="$HIVE_STACKTACH_DATABASE"
HIVE_STACKTACH_EVENTS_TABLE="events"
HIVE_STACKTACH_REQUESTS_TABLE="requests"
HIVE_STACKTACH_REQUESTS_STAGING_TABLE="${HIVE_STACKTACH_REQUESTS_TABLE}_$$_stg1"
HIVE_PROD_TABLE_DIR="$HIVE_HDFS_ROOT/${HIVE_STACKTACH_DATABASE}.db/${HIVE_STACKTACH_REQUESTS_TABLE}"


######################################################################
# Nothing below here should need changing

usage() {
  echo "Usage: $program [OPTIONS] [START-DATE] [START-DATE END-DATE]" 1>&2
  echo "  dates are in format YYYY-MM-DD" 1>&2
  echo "OPTIONS:" 1>&2
  echo "  -h  Show this help message" 1>&2
  echo "  -k  Keep temporary table" 1>&2
  echo "  -n  Show but do not execute commands" 1>&2
}


dryrun=0
keep=0
while getopts "hkn" o ; do
  case "${o}" in
    h)
     usage
     exit 0
     ;;
    k)
     keep=1
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

if [ $# -gt 1 ] ; then
  #fetch start_date, end_date from input params
  start_date=$1
  end_date=$2
else
  #fetch end_date param and calculate start_date
  end_date=$1
  end_ts=`date -u -d "$end_date 12:00:00" "+%s"`
  let start_ts=$end_ts-60*60*24*14 #minus 2 weeks
  start_date=`date -u -d @$start_ts +%Y-%m-%d`
fi

if test $start_date '>' $end_date; then
  echo "$program: ERROR: Start date $start_date is after end date $end_date" 1>&2;
  exit 1
fi

if test $dryrun = 1; then
  echo "$program: Running in DRYRUN mode - no executing" 1>&2
fi

keep_drop="DROP"
if test $keep = 1; then
  keep_drop="-- $keep_drop"
  echo "$program: Running in KEEP mode - keeping temporary files and tables" 1>&2
fi


trap "rm -f $hql_script_file $hive_out_file $hive_err_file " 0 9 15



today=`date -u +%Y-%m-%d`

echo "$program: Loading data $start_date to $end_date inclusive" 1>&2

if test $start_date = $today -o $start_date '<' $today; then
  if test $today = $end_date -o $today '<' $end_date; then
    echo "$program: ERROR: today is in range $start_date to $end_date - ending" 1>&2
    exit 1
  fi
fi


# Build HQL to create load staging table 
cat > $hql_script_file <<EOF
USE $HIVE_DATABASE;
DROP TABLE IF EXISTS $HIVE_STACKTACH_REQUESTS_STAGING_TABLE;

CREATE TABLE $HIVE_STACKTACH_REQUESTS_STAGING_TABLE AS
SELECT from_unixtime(unix_timestamp()) hive_last_updated_ts,
	min(dt) dt,
	min(to_date(cast($HIVE_STACKTACH_EVENTS_TABLE.dt_ts_utc AS TIMESTAMP))) request_dt,
	$HIVE_STACKTACH_EVENTS_TABLE.request_id,
	$HIVE_STACKTACH_EVENTS_TABLE.tenant_id,
	$HIVE_STACKTACH_EVENTS_TABLE.instance_id,
	min($HIVE_STACKTACH_EVENTS_TABLE.dt_ts_utc) request_start_ts,
	max($HIVE_STACKTACH_EVENTS_TABLE.dt_ts_utc) request_end_ts,
	max(CASE 
			WHEN $HIVE_STACKTACH_EVENTS_TABLE.old_state != 'active'
				AND $HIVE_STACKTACH_EVENTS_TABLE.STATE = 'active'
				THEN dt_ts_utc
			END) instance_active_ts,
	max(CASE 
			WHEN $HIVE_STACKTACH_EVENTS_TABLE.event_type = 'compute.instance.create.end'
				THEN 1
			ELSE 0
			END) has_end,
	max(CASE 
			WHEN $HIVE_STACKTACH_EVENTS_TABLE.event_type LIKE '%shutdown%'
				THEN 1
			ELSE 0
			END) has_shutdown,
	max(CASE 
			WHEN $HIVE_STACKTACH_EVENTS_TABLE.event_type LIKE '%delete%'
				THEN 1
			ELSE 0
			END) has_delete,
	max(CASE 
			WHEN $HIVE_STACKTACH_EVENTS_TABLE.event_type = 'compute.instance.update'
				AND $HIVE_STACKTACH_EVENTS_TABLE.host LIKE '%nova-api%'
				AND $HIVE_STACKTACH_EVENTS_TABLE.STATE = 'building'
				AND $HIVE_STACKTACH_EVENTS_TABLE.state_description = 'scheduling'
				THEN 1
			ELSE 0
			END) has_build,
	sum(CASE 
			WHEN $HIVE_STACKTACH_EVENTS_TABLE.event_type LIKE '%error%'
				THEN 1
			ELSE 0
			END) cnt_error_events,
	sum(CASE 
			WHEN $HIVE_STACKTACH_EVENTS_TABLE.STATE LIKE '%error%'
				AND $HIVE_STACKTACH_EVENTS_TABLE.old_state NOT LIKE '%error%'
				THEN 1
			ELSE 0
			END) cnt_error_states,
	sum(CASE 
			WHEN $HIVE_STACKTACH_EVENTS_TABLE.STATE NOT LIKE '%error%'
				AND $HIVE_STACKTACH_EVENTS_TABLE.old_state LIKE 'error%'
				THEN 1
			ELSE 0
			END) cnt_error_recoveries,
	concat_ws('|', collect_set($HIVE_STACKTACH_EVENTS_TABLE.STATE)) states,
	concat_ws('|', collect_set($HIVE_STACKTACH_EVENTS_TABLE.new_task_state)) task_states,
	concat_ws('|', collect_set($HIVE_STACKTACH_EVENTS_TABLE.message)) messages,
	concat_ws('|', collect_set(CASE 
				WHEN $HIVE_STACKTACH_EVENTS_TABLE.event_type LIKE '%error%'
					THEN $HIVE_STACKTACH_EVENTS_TABLE.message
				ELSE NULL
				END)) error_messages,
	concat_ws('|', collect_set(CASE 
				WHEN $HIVE_STACKTACH_EVENTS_TABLE.event_type LIKE '%error%'
					THEN $HIVE_STACKTACH_EVENTS_TABLE.exception_code
				ELSE NULL
				END)) error_exception_codes,
	concat_ws('|', collect_set($HIVE_STACKTACH_EVENTS_TABLE.event_type)) event_types,
	max(CASE 
			WHEN event_rank_asc = 1
				THEN $HIVE_STACKTACH_EVENTS_TABLE.STATE
			ELSE NULL
			END) first_state,
	max(CASE 
			WHEN event_rank_desc = 1
				THEN $HIVE_STACKTACH_EVENTS_TABLE.STATE
			ELSE NULL
			END) last_state,
	max(CASE 
			WHEN event_rank_asc = 1
				THEN $HIVE_STACKTACH_EVENTS_TABLE.state_description
			ELSE NULL
			END) first_state_description,
	max(CASE 
			WHEN event_rank_desc = 1
				THEN $HIVE_STACKTACH_EVENTS_TABLE.state_description
			ELSE NULL
			END) last_state_description,
	max(CASE 
			WHEN event_rank_asc = 1
				THEN $HIVE_STACKTACH_EVENTS_TABLE.new_task_state
			ELSE NULL
			END) first_task_state,
	max(CASE 
			WHEN event_rank_desc = 1
				THEN $HIVE_STACKTACH_EVENTS_TABLE.new_task_state
			ELSE NULL
			END) last_task_state,
	max($HIVE_STACKTACH_EVENTS_TABLE.cell) cell,
	max($HIVE_STACKTACH_EVENTS_TABLE.region) region,
	max($HIVE_STACKTACH_EVENTS_TABLE.instance_flavor_id) instance_flavor_id,
	max($HIVE_STACKTACH_EVENTS_TABLE.instance_type_name) instance_type_name,
	max($HIVE_STACKTACH_EVENTS_TABLE.instance_type_flavorid) instance_type_flavorid,
	max($HIVE_STACKTACH_EVENTS_TABLE.instance_type_id) instance_type_id,
	max($HIVE_STACKTACH_EVENTS_TABLE.instance_type_flavor_id) instance_type_flavor_id,
	max($HIVE_STACKTACH_EVENTS_TABLE.os_type) os_type,
	max($HIVE_STACKTACH_EVENTS_TABLE.image_type) image_type,
	max($HIVE_STACKTACH_EVENTS_TABLE.memory_mb) memory_mb,
	max($HIVE_STACKTACH_EVENTS_TABLE.disk_gb) disk_gb,
	max($HIVE_STACKTACH_EVENTS_TABLE.root_gb) root_gb,
	max($HIVE_STACKTACH_EVENTS_TABLE.ephemeral_gb) ephemeral_gb,
	max($HIVE_STACKTACH_EVENTS_TABLE.vcpus) vcpus,
	max($HIVE_STACKTACH_EVENTS_TABLE.instance_type) instance_type,
	max($HIVE_STACKTACH_EVENTS_TABLE.rax_options) rax_options
FROM (
	SELECT e.*,
		row_number() OVER (
			PARTITION BY e.tenant_id,
			e.instance_id,
			e.request_id ORDER BY e.dt_ts_utc ASC
			) event_rank_asc,
		row_number() OVER (
			PARTITION BY e.request_id,
			e.instance_id,
			e.tenant_id ORDER BY dt_ts_utc DESC
			) event_rank_desc
	FROM $HIVE_DATABASE.$HIVE_STACKTACH_EVENTS_TABLE e
	WHERE dt >="${start_date}" AND dt <= "${end_date}"
          AND dt_ts_utc >= "${start_date}" AND dt_ts_utc <= "${end_date}"
          AND tenant_id is not null
          AND instance_id is not null
          AND request_id is not null
	SORT BY 
		e.tenant_id,
		e.instance_id,
		e.request_id,
		e.dt_ts_utc
	) events
GROUP BY 
	$HIVE_STACKTACH_EVENTS_TABLE.request_id,
	$HIVE_STACKTACH_EVENTS_TABLE.instance_id,
	$HIVE_STACKTACH_EVENTS_TABLE.tenant_id;
EOF

echo "$program: Creating $HIVE_STACKTACH_REQUESTS_STAGING_TABLE table by $hql_script_file" 1>&2
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
    echo "$program: FAILED load to staging table hive $cmd with result $status" 1>&2
    exit $status
  fi
fi


# Build HQL to create requests table and load ETL output files into it
cat > $hql_script_file <<EOF
----Table definition for stacktack requests derived from aggregated events data using stacktach.events dataset---- 
----This ETL is set to retrieve requests from events received from stacktach in past 15 days , so everyday past 15 days partitions are reloaded ----- 
----Fields used to derive % build success rate are [has_build,has_end]-----
----Fields used to analyse failed or  not failed but not completed builds [has_build,has_end,has_shutdown,has_delete,cnt_error*,error_messages]
----Fields used to analyse lifecycle of any instance are [instance_id,request_id,states,task_states,request_start_ts,request_end_ts ]
----Fields used to derive avg duration for successful builds requests are [has_build,has_end,request_end_ts - request_start_ts]
----Fields used to derive avg duration for the 1st occurrence of an instance in a day to land in an active state are [instance_id , dt ,has_build ,min(instance_active_ts)- min(request_start_ts) ]
----All other textual fields can be used for aggregation and summary. 

USE $HIVE_DATABASE;

CREATE TABLE if not exists $HIVE_STACKTACH_REQUESTS_TABLE (
	hive_last_updated_ts TIMESTAMP,
	request_dt STRING,
	request_id STRING,
	tenant_id STRING,
	instance_id STRING,
	request_start_ts TIMESTAMP,
	request_end_ts TIMESTAMP,
	instance_active_ts TIMESTAMP,
	has_end TINYINT,
	has_shutdown TINYINT,
	has_delete TINYINT,
	has_build TINYINT,
	cnt_error_events INT,
	cnt_error_states INT,
	cnt_error_recoveries INT,
	states STRING,
	task_states STRING,
	messages STRING,
	error_messages STRING,
	error_exception_codes STRING,
	event_types STRING,
	first_state STRING,
	last_state STRING,
	first_state_description STRING,
	last_state_description STRING,
	first_task_state STRING,
	last_task_state STRING,
	cell STRING,
	region STRING,
	instance_flavor_id STRING,
	instance_type_name STRING,
	instance_type_flavorid STRING,
	instance_type_id STRING,
	instance_type_flavor_id STRING,
	os_type STRING,
	image_type STRING,
	memory_mb STRING,
	disk_gb STRING,
	root_gb STRING,
	ephemeral_gb STRING,
	vcpus STRING,
	instance_type STRING,
	rax_options STRING
	) partitioned BY (dt STRING) row format delimited fields terminated BY ',' escaped BY '\\\\' COLLECTION items terminated BY '|' map keys terminated BY '=' lines terminated BY '\n' stored AS orc;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.compress.output=true;
SET hive.exec.max.dynamic.partitions=2000;
SET mapred.max.split.size=521000000;
SET mapred.output.compression.type=BLOCK;
SET io.sort.mb=256;
SET io.sort.factor=100;
SET mapred.job.reuse.jvm.num.tasks=-1;
SET hive.enforce.sorting=true;
SET mapreduce.reduce.input.limit = -1;
SET hive.merge.mapredfiles = true;
SET mapred.job.reduce.memory.mb=4096;
SET mapred.job.map.memory.mb=4096;

INSERT overwrite TABLE $HIVE_STACKTACH_REQUESTS_TABLE PARTITION (dt)
SELECT hive_last_updated_ts,
	request_dt,
	request_id,
	tenant_id,
	instance_id,
	request_start_ts,
	request_end_ts,
	instance_active_ts,
	has_end,
	has_shutdown,
	has_delete,
	has_build,
	cnt_error_events,
	cnt_error_states,
	cnt_error_recoveries,
	states,
	task_states,
	messages,
	error_messages,
	error_exception_codes,
	event_types,
	first_state,
	last_state,
	first_state_description,
	last_state_description,
	first_task_state,
	last_task_state,
	cell,
	region,
	instance_flavor_id,
	instance_type_name,
	instance_type_flavorid,
	instance_type_id,
	instance_type_flavor_id,
	os_type,
	image_type,
	memory_mb,
	disk_gb,
	root_gb,
	ephemeral_gb,
	vcpus,
	instance_type,
	rax_options,
	dt
FROM $HIVE_STACKTACH_REQUESTS_STAGING_TABLE;

$keep_drop TABLE $HIVE_STACKTACH_REQUESTS_STAGING_TABLE;
EOF

echo "$program: Creating/Loading $HIVE_STACKTACH_REQUESTS_TABLE table by $hql_script_file" 1>&2
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
    echo "$program: FAILED to create/load $HIVE_STACKTACH_REQUESTS_TABLE hive $cmd with result $status" 1>&2
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
