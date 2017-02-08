#!/bin/bash
#
# Load the stacktach data for a given DATE
#
# USAGE: stacktach-load-date.sh [OPTIONS] YYYY-MM-DD [YYYY-MM-DD]
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
raw_data_file_list="/tmp/${program}-$$-stacktach-data-files.lst"
input_data_files_list="/tmp/${program}-$$-input-data-files.lst"
hql_script_file="/tmp/${program}-$$-script.hql"
db_files_list="/tmp/${program}-$$-db-files.lst"
hive_out_file="/tmp/${program}-$$-hive-out.log"
hive_err_file="/tmp/${program}-$$-hive-err.log"
dt_files_list="/tmp/${program}-$$-dt-files.lst"
dates_list="/tmp/${program}-$$-dates.lst"

# HDFS config
HDFS_WORKING_DIR="/tmp/${program}-$$"
HDFS_JSON_TMP_DIR="${HDFS_WORKING_DIR}/json-in"
HDFS_DATA_INPUT_DIR="$HDFS_STACKTACH_INPUT_DIR"
HDFS_ETL_OUTPUT_PREFIX="$STACKTACH_ETL_OUTPUT_PREFIX"

# Hive config
HIVE_DATABASE="$HIVE_STACKTACH_DATABASE"
HIVE_TABLE="$HIVE_STACKTACH_TABLE"
HIVE_PROD_TABLE="$HIVE_TABLE"
HIVE_STAGING1_TABLE="${HIVE_TABLE}_$$_stg1"
HIVE_STAGING2_TABLE="${HIVE_TABLE}_$$_stg2"
HIVE_TMP_JSON_EVENTS_TABLE="${HIVE_TABLE}_$$_tmp_json_events"
HIVE_TMP_NOTIFICATIONS_TABLE="${HIVE_TABLE}_$$_tmp_notifications"
HIVE_TMP_EVENTS_TABLE="${HIVE_TABLE}_$$_tmp_events"
HIVE_TMP_EVENTS_PAYLOAD_TABLE="${HIVE_TABLE}_$$_tmp_events_payload"
HIVE_TMP_EVENTS_PAYLOAD_IMAGEMETA_TABLE="${HIVE_TABLE}_$$_tmp_events_payload_imagemeta"
HIVE_TMP_EVENTS_INSTANCE_TABLE="${HIVE_TABLE}_$$_tmp_events_instance"
HIVE_TMP_EVENTS_EXCEPTION_TABLE="${HIVE_TABLE}_$$_tmp_events_exception"
HIVE_PROD_TABLE_DIR="$HIVE_HDFS_ROOT/${HIVE_STACKTACH_DATABASE}.db/${HIVE_PROD_TABLE}"


######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

usage() {
  echo "Usage: $program [OPTIONS] START-DATE [END-DATE]" 1>&2
  echo "  dates are in format YYYY-MM-DD" 1>&2
  echo "OPTIONS:" 1>&2
  echo "  -h  Show this help message" 1>&2
  echo "  -k  Keep temporary files and tables" 1>&2
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

keep_drop="DROP"
if test $keep = 1; then
  keep_drop="-- $keep_drop"
  echo "$program: Running in KEEP mode - keeping temporary files and tables" 1>&2
fi


trap "rm -f $raw_data_file_list $input_data_files_list $hql_script_file $db_files_list $hive_out_file $hive_err_file $dt_files_list $dates_list" 0 9 15



today=`date -u +%Y-%m-%d`

echo "$program: Loading data $start_date to $end_date inclusive" 1>&2

if test $start_date = $today -o $start_date '<' $today; then
  if test $today = $end_date -o $today '<' $end_date; then
    echo "$program: ERROR: today is in range $start_date to $end_date - ending" 1>&2
    exit 1
  fi
fi


echo "$program: Looking for input files in $HDFS_DATA_INPUT_DIR:" 1>&2
echo "  $start_date ... $end_date: all files" 1>&2

start_date_nom=`echo $start_date | tr -d '-'`
end_date_nom=`echo $end_date | tr -d '-'`

# Filenames like: 20150519-ord-1431993607.132478-aggregate.json.gz
# Filenames like: <YYYYMMDD>-<REGION>-<TS_SEC.TS_USEC>-aggregate.json.gz
#
# Output file format: <date-from-path> \t <full-hdfs-path> \t <file-name>
hadoop fs -ls $HDFS_DATA_INPUT_DIR | \
  awk '!/^Found/ { print $8 }' | \
  sed -e 's,^\(.*/\)\([0-9]*\)\(.*\),\2\t\1\2\3\t\2\3,' \
  > $raw_data_file_list

awk "{if(\$1 >= \"${start_date_nom}\" && \$1 <= \"${end_date_nom}\") { print \$2 } }" < $raw_data_file_list  > $input_data_files_list
awk "{if(\$1 >= \"${start_date_nom}\" && \$1 <= \"${end_date_nom}\") { print \$1 } }" < $raw_data_file_list |sort -u > $dates_list

input_files_count=`wc -l < $input_data_files_list`

if test $input_files_count -eq 0; then
  echo "$program: No data available for date range $start_date ... $end_date" 1>&2
  exit 1
fi

dates=`sed -e 's/^\(....\)\(..\)\(..\) */\1-\2-\3/' < $dates_list | tr '\012' ' '`

echo "$program: Found $input_files_count files for date range $start_date ... $end_date" 1>&2
if test $dryrun = 1; then
  echo "$program: Would process these input files" 1>&2
  sed -e 's/^/    /' $input_data_files_list  1>&2
fi

# Delete working space
cmd="hadoop fs -rmr $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  $cmd && true
fi

# Summarize the resulting files in the Hive DB
echo "$program: Processing loading $input_files_count files into $HIVE_TABLE" 1>&2
echo "     Source: $HDFS_DATA_INPUT_DIR" 1>&2
  sed -e 's/^/    /' $input_data_files_list  1>&2
echo "  Staging 1: Hive table $HIVE_STAGING1_TABLE" 1>&2
echo "  Staging 2: Hive table $HIVE_STAGING2_TABLE" 1>&2
echo "       Dest: Hive table $HIVE_PROD_TABLE" 1>&2

# Copy JSON files into temporary working directory for EXTERNAL TABLE
cmd="hadoop fs -mkdir $HDFS_JSON_TMP_DIR"
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
for dt in $dates; do
  dt_nom=`echo $dt | tr -d '-'`
  dt_dir="$HDFS_JSON_TMP_DIR/dt=${dt}"
  cmd="hadoop fs -mkdir $dt_dir"
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
  awk "{if(\$1 == \"${dt_nom}\") { print \$2 } }" < $raw_data_file_list  > $dt_files_list
  dt_files_count=`wc -l < $dt_files_list`
  dt_files=`tr '\012' ' ' < $dt_files_list | sed -e 's/,$//'`

  echo "$program: Date $dt copying $dt_files_count files to partition dir $dt_dir" 1>&2

  cmd="hadoop fs -cp $dt_files $dt_dir/"
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
done

# Build HQL to create staging table and load ETL output files into it
cat > $hql_script_file <<EOF
SET hive.hadoop.supports.splittable.combineinputformat = TRUE;

USE ${HIVE_DATABASE};

DROP TABLE IF EXISTS ${HIVE_STAGING1_TABLE};

CREATE EXTERNAL TABLE ${HIVE_STAGING1_TABLE} (
        json_notification STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
        LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '$HDFS_JSON_TMP_DIR';
;
EOF

# Add to HQL to load external table partitions for each date
for dt in $dates; do
  cat >> $hql_script_file <<EOF
ALTER TABLE ${HIVE_STAGING1_TABLE} ADD PARTITION (dt = '${dt}');
EOF
done


echo "$program: Running load staging 1 hive HQL from $hql_script_file" 1>&2
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
    echo "$program: FAILED load to staging 1 hive $cmd with result $status" 1>&2
    exit $status
  fi
fi


# Build HQL to create second staging table and load ETL output files into it
cat > $hql_script_file <<EOF
SET hive.hadoop.supports.splittable.combineinputformat = TRUE;

USE ${HIVE_DATABASE};

DROP TABLE IF EXISTS ${HIVE_STAGING2_TABLE};

DROP TABLE IF EXISTS ${HIVE_TMP_JSON_EVENTS_TABLE};
DROP TABLE IF EXISTS ${HIVE_TMP_NOTIFICATIONS_TABLE};
DROP TABLE IF EXISTS ${HIVE_TMP_EVENTS_TABLE};
DROP TABLE IF EXISTS ${HIVE_TMP_EVENTS_PAYLOAD_TABLE};
DROP TABLE IF EXISTS ${HIVE_TMP_EVENTS_PAYLOAD_IMAGEMETA_TABLE};
DROP TABLE IF EXISTS ${HIVE_TMP_EVENTS_INSTANCE_TABLE};
DROP TABLE IF EXISTS ${HIVE_TMP_EVENTS_EXCEPTION_TABLE};


CREATE TABLE ${HIVE_TMP_JSON_EVENTS_TABLE}
AS
        SELECT
          dt
        , INPUT__FILE__NAME file
        , a.json_notification json_notification
	FROM
	${HIVE_STAGING1_TABLE} a;

$keep_drop TABLE ${HIVE_STAGING1_TABLE};

CREATE TABLE ${HIVE_TMP_NOTIFICATIONS_TABLE}
AS
        -- dt, file, json_notification
	SELECT *
	FROM
	${HIVE_TMP_JSON_EVENTS_TABLE} a
	LATERAL VIEW
	JSON_TUPLE(a.json_notification, 'cell', 'region', 'notification') b
AS cell, region, notification;

$keep_drop TABLE ${HIVE_TMP_JSON_EVENTS_TABLE};

CREATE TABLE ${HIVE_TMP_EVENTS_TABLE}
AS
        -- dt, file, json_notification, cell, region, notification
	SELECT *
	FROM
	${HIVE_TMP_NOTIFICATIONS_TABLE} a
	LATERAL VIEW
	JSON_TUPLE(a.notification, 'message_id', '_context_request_id', 'timestamp', 'event_type', 'tenant_id', '_context_project_id', 'publisher_id', 'payload', 'exception', 'instance', 'priority') b
AS message_id, context_request_id, timestamp, event_type, tenant_id, context_project_id, publisher_id, payload, exception, instance, priority;

$keep_drop TABLE ${HIVE_TMP_NOTIFICATIONS_TABLE};

CREATE TABLE ${HIVE_TMP_EVENTS_PAYLOAD_TABLE}
AS
        -- dt, file, json_notification, cell, region, notification, message_id, context_request_id, timestamp, event_type, tenant_id, context_project_id, publisher_id, payload, exception, instance, priority
	SELECT *
	FROM
	${HIVE_TMP_EVENTS_TABLE} a
	LATERAL VIEW
	JSON_TUPLE(a.payload, 'message', 'tenant_id', 'instance_uuid', 'instance_type', 'instance_id', 'user_id', 'instance_flavor_id', 'instance_type_id', 'os_type', 'memory_mb', 'root_gb', 'disk_gb', 'ephemeral_gb', 'vcpus', 'audit_period_beginning', 'audit_period_ending', 'state', 'old_state', 'state_description', 'launched_at', 'deleted_at', 'terminated_at', 'display_name', 'image_meta', 'terminated_at_utc','publisher_id','cell_name','new_task_state','old_task_state','progress','image_ref_url') b
AS  message, tenant_id_1, instance_uuid, instance_type, instance_id, user_id, instance_flavor_id, instance_type_id, os_type, memory_mb, root_gb, disk_gb, ephemeral_gb, vcpus, audit_period_beginning, audit_period_ending, state, old_state, state_description, launched_at, deleted_at, terminated_at, display_name, image_meta, terminated_at_utc, publisher_id1, cell_name, new_task_state, old_task_state, progress, image_ref_url;

$keep_drop TABLE  IF EXISTS ${HIVE_TMP_EVENTS_TABLE};

CREATE TABLE ${HIVE_TMP_EVENTS_PAYLOAD_IMAGEMETA_TABLE}
AS
        -- dt, file, json_notification, cell, region, notification, message_id, context_request_id, timestamp, event_type, tenant_id, context_project_id, publisher_id, payload, exception, instance, priority, message, tenant_id_1, instance_uuid, instance_type, instance_id, user_id, instance_flavor_id, instance_type_id, os_type, memory_mb, root_gb, disk_gb, ephemeral_gb, vcpus, audit_period_beginning, audit_period_ending, state, old_state, state_description, launched_at, deleted_at,  terminated_at, display_name, image_meta, terminated_at_utc, publisher_id1, cell_name, new_task_state, old_task_state, progress, image_ref_url
	SELECT *
	FROM
	${HIVE_TMP_EVENTS_PAYLOAD_TABLE} a
	LATERAL VIEW
	JSON_TUPLE(a.image_meta, 'instance_type_name', 'instance_type_flavorid', 'instance_type_flavor_id', 'org.openstack__1__architecture', 'org.openstack__1__os_version', 'org.openstack__1__os_distro', 'com.rackspace__1__options', 'image_type' ) b
AS  instance_type_name,instance_type_flavorid,instance_type_flavor_id,org_openstack__1__architecture,org_openstack__1__os_version,org_openstack__1__os_distro,com_rackspace__1__options,image_type;

$keep_drop TABLE ${HIVE_TMP_EVENTS_PAYLOAD_TABLE};

CREATE TABLE ${HIVE_TMP_EVENTS_INSTANCE_TABLE}
AS
        -- dt, file, json_notification, cell, region, notification, message_id, context_request_id, timestamp, event_type, tenant_id, context_project_id, publisher_id, payload, exception, instance, priority, message, tenant_id_1, instance_uuid, instance_type, instance_id, user_id, instance_flavor_id, instance_type_id, os_type, memory_mb, root_gb, disk_gb, ephemeral_gb, vcpus, audit_period_beginning, audit_period_ending, state, old_state, state_description, launched_at, deleted_at, terminated_at, display_name, image_meta, terminated_at_utc, publisher_id1, cell_name, new_task_state, old_task_state, progress, image_ref_url, instance_type_name, instance_type_flavorid, instance_type_flavor_id, org_openstack__1__architecture, org_openstack__1__os_version, org_openstack__1__os_distro, com_rackspace__1__options, image_type
	SELECT *
	FROM
	${HIVE_TMP_EVENTS_PAYLOAD_IMAGEMETA_TABLE} a
	LATERAL VIEW
	JSON_TUPLE ( a.INSTANCE, 'uuid') b
AS  uuid;

$keep_drop TABLE ${HIVE_TMP_EVENTS_PAYLOAD_IMAGEMETA_TABLE};

CREATE TABLE ${HIVE_TMP_EVENTS_EXCEPTION_TABLE}
AS
        -- dt, file, json_notification, cell, region, notification, message_id, context_request_id, timestamp, event_type, tenant_id, context_project_id, publisher_id, payload, exception, instance, priority, message, tenant_id_1, instance_uuid, instance_type, instance_id, user_id, instance_flavor_id, instance_type_id, os_type, memory_mb, root_gb, disk_gb, ephemeral_gb, vcpus, audit_period_beginning, audit_period_ending, state, old_state, state_description, launched_at, deleted_at, terminated_at, display_name, image_meta, terminated_at_utc, publisher_id1, cell_name, new_task_state, old_task_state, progress, image_ref_url, instance_type_name, instance_type_flavorid, instance_type_flavor_id, org_openstack__1__architecture, org_openstack__1__os_version, org_openstack__1__os_distro, com_rackspace__1__options, image_type, uuid
	SELECT *
	FROM
	${HIVE_TMP_EVENTS_INSTANCE_TABLE} a
	LATERAL VIEW
	JSON_TUPLE ( a.exception, 'kwargs') b
AS  kwargs;

$keep_drop TABLE ${HIVE_TMP_EVENTS_INSTANCE_TABLE};

CREATE TABLE ${HIVE_STAGING2_TABLE}
AS
        -- dt, file, json_notification, cell, region, notification, message_id, context_request_id, timestamp, event_type, tenant_id, context_project_id, publisher_id, payload, exception, instance, priority, message, tenant_id_1, instance_uuid, instance_type, instance_id, user_id, instance_flavor_id, instance_type_id, os_type, memory_mb, root_gb, disk_gb, ephemeral_gb, vcpus, audit_period_beginning, audit_period_ending, state, old_state, state_description, launched_at, deleted_at, terminated_at, display_name, image_meta, terminated_at_utc, publisher_id1, cell_name, new_task_state, old_task_state, progress, image_ref_url, instance_type_name, instance_type_flavorid, instance_type_flavor_id, org_openstack__1__architecture, org_openstack__1__os_version, org_openstack__1__os_distro, com_rackspace__1__options, image_type, uuid, kwargs
	SELECT *
	FROM
	${HIVE_TMP_EVENTS_EXCEPTION_TABLE} a
	LATERAL VIEW
	JSON_TUPLE ( a.kwargs, 'code', 'uuid') b
AS  exe_code, exe_uuid_1;

$keep_drop TABLE ${HIVE_TMP_EVENTS_EXCEPTION_TABLE};

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


FROM ${HIVE_STAGING2_TABLE}
INSERT OVERWRITE TABLE ${HIVE_PROD_TABLE} PARTITION(dt)
SELECT
	file
,	cell
,	region
,	CAST(timestamp AS TIMESTAMP)  dt_ts_utc
,	TO_DATE(CAST(timestamp AS TIMESTAMP)) dt_utc
,	timestamp AS notification_timestamp
,	event_type	event_type
,	COALESCE (tenant_id_1, tenant_id, context_project_id) tenant_id
,	user_id user_id
,	context_request_id request_id
,	message_id message_id
,	message message
,	COALESCE (instance_uuid, instance_id, exe_uuid_1 ,uuid) instance_id
,	COALESCE (publisher_id,publisher_id1) host
,	COALESCE (publisher_id,publisher_id1) service
,	COALESCE(instance_type, instance_type_name, instance_type_flavorid) instance_flavor
,	instance_flavor_id instance_flavor_id
,	instance_type_name instance_type_name
,	instance_type_flavorid instance_type_flavorid
,	instance_type_id instance_type_id
,	instance_type_flavor_id instance_type_flavor_id
,	os_type os_type
,	image_type image_type
,	memory_mb memory_mb
,	disk_gb disk_gb
,	root_gb root_gb
,	ephemeral_gb ephemeral_gb
,	vcpus vcpus
,	instance_type instance_type
,	state state
,	old_state
,	state_description state_description
,	org_openstack__1__architecture os_architecture
,	org_openstack__1__os_version os_version
,	org_openstack__1__os_distro os_distro
,	com_rackspace__1__options rax_options
,	launched_at launched_at_utc
,	deleted_at deleted_at_utc
,	display_name display_name
,	audit_period_beginning audit_period_beginning_utc
,	audit_period_ending audit_period_ending_utc
,	exe_code exception_code
,	terminated_at_utc
,	priority
,	COALESCE(publisher_id, publisher_id1) publisher_id
,	cell_name
,	new_task_state
,	old_task_state
,	progress
,	image_ref_url
,       dt
WHERE dt LIKE '20%'
;

$keep_drop TABLE IF EXISTS ${HIVE_STAGING2_TABLE};

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
cmd="hadoop fs -rmr $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
elif test $keep = 1; then
  echo "$program: preserving working dir $HDFS_WORKING_DIR" 1>&2
else
  echo "$program: running $cmd"
  $cmd
  status=$?
  if test $status != 0; then
    echo "$program: FAILED $cmd with result $status" 1>&2
    exit $status
  fi
fi

if test $keep = 1; then
  echo "$program: Kept working dir $HDFS_WORKING_DIR and temporary tables with prefix ${HIVE_TABLE}_$$" 1>&2
  echo "$program: Kept temporary tables in $HIVE_DATABASE with prefix ${HIVE_TABLE}_$$" 1>&2
fi
