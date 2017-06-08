#!/bin/sh
#
# Load the latest (by date) clickstream CSV schema data from the daily
# upload into Hive tables in Hive database 'clickstream'
#
# USAGE: clickstream-account-load.sh [OPTIONS]
# -n/--dryrun : show but do not execute
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

# HDFS config

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



trap "rm -f $hql_script_file $hive_out_file $hive_err_file" 0 9 15

echo "$program: Looking for input files in $HDFS_CLICKSTREAM_SCHEMA_DIR" 1>&2

# For each file, load into the appropriate table
for prefix_table in $HIVE_CLICKSTREAM_TABLE_CONFIG; do
  prefix=`echo $prefix_table | cut -d : -f 1`
  table=`echo $prefix_table | cut -d : -f 2`

  prod_table=$table

  # HDFS
  table_csv_file="$HDFS_CLICKSTREAM_SCHEMA_DIR/$prefix.csv"
  prod_table_dir="$HIVE_HDFS_ROOT/${HIVE_CLICKSTREAM_DATABASE}.db/${table}"
  tmp_csv_file="/tmp/${program}-$$-${table}.csv"

  echo "$program: Loading $table_csv_file into Hive table $prod_table" 1>&2

  hadoop fs -cp $table_csv_file $tmp_csv_file
  
  # Build HQL to load prod table
  cat > $hql_script_file <<EOF
USE ${HIVE_CLICKSTREAM_DATABASE};

ADD JAR ${CSV_SERDE_JAR};

LOAD DATA INPATH '${tmp_csv_file}' OVERWRITE INTO TABLE ${prod_table};

EOF

  echo "$program: Running load hive HQL from $hql_script_file" 1>&2
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
      echo "$program: FAILED load hive $cmd with result $status" 1>&2
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
USE ${HIVE_CLICKSTREAM_DATABASE};

ADD JAR ${CSV_SERDE_JAR};

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
