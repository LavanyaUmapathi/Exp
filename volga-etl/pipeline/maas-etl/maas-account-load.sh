#!/bin/sh
#
# Load the latest (by date) MAAS configuration data from the nightly
# account-data dump into Hive tables
#
# USAGE: maas-account-load.sh [OPTIONS]
# -n/--dryrun : show but do not execute
#


if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. /etc/acumen/config.sh


program=`basename $0`

# ETL config
EVENT_TYPE='configuration'

# File systejm
account_data_file_list="/tmp/${program}-$$-account-data-files.lst"
input_data_file_list="/tmp/${program}-$$-input-data-files.lst"
input_data_all_file_list="/tmp/${program}-$$-input-data-all-files.lst"
db_files_list="/tmp/${program}-$$-db-files.lst"
hql_script_file="/tmp/${program}-$$-script.hql"
hive_out_file="/tmp/${program}-$$-hive-out.log"
hive_err_file="/tmp/${program}-$$-hive-err.log"

# HDFS config
HDFS_WORKING_DIR="/tmp/${program}-$$"
HDFS_MAAS_ACCOUNT_DATA_INPUT_DIR="$HDFS_MAAS_HOME/account-data"


######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

dryrun=0
for arg in "$@"; do
  case $arg in
    -n|--dryrun)
     dryrun=1
     shift
     ;;
    -*)
      echo "$program: Unknown option $arg" 1>&2
      exit 1
      ;;
    *)
      ;;
   esac
done



trap "rm -f $account_data_file_list $input_data_file_list $input_data_all_file_list $db_files_list $hql_script_file $hive_out_file $hive_err_file" 0 9 15


echo "$program: Looking for input files in $HDFS_MAAS_ACCOUNT_DATA_INPUT_DIR:" 1>&2

# Output file format: <date-from-path> \t <full-hdfs-path> \t <file-name>
hadoop fs -ls $HDFS_MAAS_ACCOUNT_DATA_INPUT_DIR | \
  awk '!/^Found/ { print $8 }' | \
  sed -e 's,^\(.*/\)\([0-9]*\)\(.*\),\2\t\1\2\3\t\2\3,' \
  > $account_data_file_list

awk '{print $2}' $account_data_file_list > $input_data_all_file_list
maas_accounts_partition_date=`awk '{print $1}' $account_data_file_list | tail -1`
input_files_count=`wc -l < $input_data_all_file_list`

if test $input_files_count -eq 0; then
  echo "$program: No account data available in $HDFS_MAAS_ACCOUNT_DATA_INPUT_DIR" 1>&2
  exit 0
fi

# Just load newest
tail -1 $input_data_all_file_list > $input_data_file_list

if test $dryrun = 1; then
  echo "$program: Would process (out of $input_files_count)" 1>&2
  sed -e 's/^/    /' $input_data_file_list  1>&2
fi


# Delete working space
cmd="hadoop fs -rmr $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  $cmd && exit 0
fi


# Run the Map-Reduce job to read the latest configuration file from the
# MAAS account-data area and generate it as files below $HDFS_WORKING_DIR
echo "$program: Running Map-Reduce job to load configuration" 1>&2
cmd="yarn jar $MAAS_ETL_JAR $MAAS_ETL_JAR_CLASS $MAAS_ETL_JAR_OPTS -o $HDFS_WORKING_DIR -if $input_data_file_list -of text -et $EVENT_TYPE"
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


# For each set of files, load into the appropriate table
for event_table in $HIVE_MAAS_TABLE_CONFIG; do
  EVENT_TYPE=`echo $event_table | cut -d : -f 1`
  table=`echo $event_table | cut -d : -f 2`

  file_glob="$HDFS_WORKING_DIR/${EVENT_TYPE}*"
  prod_table=$table
  staging_table="${table}_stg"
  prod_table_dir="$HIVE_HDFS_ROOT/${HIVE_MAAS_DATABASE}.db/${table}"

  # Check the ETL job outputs
  hadoop fs -ls $file_glob 2>/dev/null | sed -e 's/^/    /' > $db_files_list
  db_files_list_count=`wc -l < $db_files_list`

  if test $db_files_list_count -eq 0 -a $dryrun = 0; then
    echo "$program: ERROR: ETL job created no $file_glob output files" 1>&2
    hadoop fs -rmr $HDFS_WORKING_DIR
    exit 1
  fi

  echo "$program: Processing loading $db_files_list_count files into $table"
  echo "   Source: $file_glob" 1>&2
  cat $db_files_list
  echo "  Staging: Hive table $staging_table" 1>&2
  echo "     Dest: Hive table $table" 1>&2

  # Build script with updated load path
  template_file="$MAAS_ETL_HQLS/${prod_table}.hql"
  sed -e "s,inpath '[^']*',inpath '${file_glob}',i" \
      -e "/^CREATE TABLE ${prod_table} *(/,/STORED AS ORC/d" \
    $template_file > $hql_script_file

  echo "$program: Running load to staging hive HQL from $hql_script_file" 1>&2
  cmd="hive $HIVE_OPTS --define PARTITION_DATE=$maas_accounts_partition_date -f $hql_script_file"
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

  cmd="hadoop fs -chmod -R 755 ${prod_table_dir}"
  if test $dryrun = 1; then
    echo "$program: DRYRUN: would run $cmd" 1>&2
  else
    $cmd
  fi

  cat > $hql_script_file <<EOF
USE ${HIVE_MAAS_DATABASE};

SELECT COUNT(1) from ${prod_table};
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
      echo "$program: FAILED validate hive $cmd with result $status" 1>&2
      exit $status
    fi
    echo "$program: Validation hive returned:" 1>&2
    sed -e 's/^/    /' $hive_out_file 1>&2
  fi
done

# Delete working dir
cmd="hadoop fs -rmr $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  echo "$program: Deleting working dir" 1>&2
  $cmd
fi

# Delete all input files after successful run
files_to_delete=`cat $input_data_all_file_list`

cmd="hadoop fs -rmr -skipTrash $files_to_delete"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would delete all input files" 1>&2
  echo $cmd 1>&2
else
  echo "$program: Deleting all input files" 1>&2
  $cmd && true
fi
