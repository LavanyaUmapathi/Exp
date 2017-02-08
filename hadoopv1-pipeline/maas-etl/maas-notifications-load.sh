#!/bin/sh
#
# Load all MAAS notifications data
#
# USAGE: maas-notifications-load.sh [OPTIONS]
# -n/--dryrun : show but do not execute
#


if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. $ADMIN/cron/config.sh


program=`basename $0`

# File system
hql_script_file="/tmp/${program}-$$-script.hql"
db_files_list="/tmp/${program}-$$-db-files.lst"
hive_out_file="/tmp/${program}-$$-hive-out.log"
hive_err_file="/tmp/${program}-$$-hive-err.log"

# HDFS config
HDFS_WORKING_DIR="/tmp/${program}-$$"


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


trap "rm -f $db_files_list $hql_script_file $hive_out_file $hive_err_file" 0 9 15


# Delete working space
cmd="hadoop fs -rmr $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  $cmd && exit 0
fi

EVENT_TYPE='notification'


# Run the Map-Reduce job to load all notifications JSON files from the
# MAAS notification-data area and write it as files below $HDFS_WORKING_DIR
echo "$program: Running Map-Reduce job to load notifications" 1>&2
pattern=".+"
cmd="yarn jar $MAAS_ETL_JAR $MAAS_ETL_JAR_CLASS $MAAS_ETL_JAR_OPTS  $MAAS_ETL_NOTIFICATIONS_LOAD_JAR_OPTS -o $HDFS_WORKING_DIR -i $HDFS_MAAS_HOME/notification-data -of text -et $EVENT_TYPE"
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


file='notification'
table='notifications'

file_glob="$HDFS_WORKING_DIR/${EVENT_TYPE}*"
prod_table=$table
staging_table="${table}_stg"
prod_table_dir="$HIVE_HDFS_ROOT/${HIVE_MAAS_DATABASE}.db/${table}"


# Check the ETL job outputs
hadoop fs -ls $file_glob 2>/dev/null | sed -e 's/^/    /' > $db_files_list
db_files_list_count=`wc -l < $db_files_list`

if test $db_files_list_count -eq 0; then
  echo "$program: ERROR: ETL job created no $file_glob files for date $date" 1>&2
  hadoop fs -rmr $HDFS_WORKING_DIR
  exit 1
fi

echo "$program: Processing loading $db_files_list_count files into $table"
echo "   Source: $file_glob" 1>&2
hadoop fs -ls $file_glob 2>/dev/null | sed -e 's/^/    /'
echo "  Staging: Hive table $staging_table" 1>&2
echo "     Dest: Hive table $table" 1>&2

# Build script with updated load path
template_file="$MAAS_ETL_HQLS/${prod_table}.hql"
sed -e "s,inpath '[^']*',inpath '${file_glob}',i" \
    -e "/^CREATE TABLE ${prod_table} *(/,/STORED AS ORC/d" \
  $template_file > $hql_script_file

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

# hadoop fs -chown -R $HDFS_MAAS_USER:$HDFS_MAAS_USER ${prod_table_dir}
hadoop fs -chmod -R 755 ${prod_table_dir}

cat > $hql_script_file <<EOF
USE ${HIVE_MAAS_DATABASE};

SELECT dt, COUNT(1) AS c from ${prod_table}
GROUP BY dt
ORDER BY dt ASC
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


# Delete working dir
cmd="hadoop fs -rmr $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  echo "$program: running $cmd"
  $cmd
  if test $status != 0; then
    echo "$program: FAILED $cmd with result $status" 1>&2
    exit $status
  fi
fi