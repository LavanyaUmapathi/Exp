usage="$(basename "$0") db_name table_name pk_columns, ts_column, isTimestamp, isMilliSeconds"
if [[ $1 == "-h" ]]
then
  echo $usage
  echo "The script deduplicates last two years of data"
  echo "Dry run available as:  $(basename "$0") -dry"
  exit
fi
dry=

if [[ $1 == "-dry" ]]
then
   dry="echo"

fi

set -e
export HIVE_HOME=/usr/hdp/current/hive-client
export HADOOP_HOME=/usr/hdp/current/hadoop-client
#export AIRFLOW_RUN_DIR=/home/airflow/airflow-jobs/scripts
#export RUN_DIR=/var/run/ods_archive
db_name=$1
subject=$2
pk_columns=$3
ts_column=$4
is_timestamp=$5
is_milliseconds=$6
if [[ $dry == "echo" ]]
then
        db_name=$2
        subject=$3
        pk_columns=$4
        ts_column=$5
        is_timestamp=$6
        is_milliseconds=$7

fi
echo $db_name
echo $subject
echo $pk_columns
echo $is_timestamp
echo $is_milliseconds
echo $ts_column
tmp_table_name=tmp_$subject
tmp1_table_name=tmp1_$subject
echo $tmp_table_name
echo $tmp1_table_name
latest="_latest"
final_table_name="$subject$latest"
orc="_orc"
orc_table_name="$subject$orc"
echo $orc_table_name
echo $final_table_name
curr_date=$(date  +%Y-%m-%d)
echo $curr_date
curr_date_ts=$(date -d "$currDate " +%s)
echo $curr_date_ts
curr_date_ts_ms=$(($curr_date_ts * 1000))
echo  $curr_date_ts_ms
two_years_old_date=$(date -d "$curr_date -730day" +%Y-%m-%d)
echo $two_years_old_date
two_years_old_ts=$(date -d "$two_years_old_date" +%s)
echo $two_years_old_ts
two_years_old_ts_ms=$(($two_years_old_ts * 1000))
echo $two_years_old_ts_ms
join_condition=''
IFS=',' read -r -a pk_fields <<<"$pk_columns"
for el in "${pk_fields[@]}"
do
        if [[ $join_condition == '' ]]
        then
                join_condition=" a.$el = b.$el "
        else
                join_condition="$join_condition and a.$el = b.$el "
        fi
done
join_condition="$join_condition and a.$ts_column = b.$ts_column "
echo $join_condition
echo "set hive.stats.autogather=false;" >> /tmp/scripts/remove_dups_$db_name_$subject
echo "drop table if exists $db_name.$tmp_table_name;" > /tmp/scripts/remove_dups_$db_name_$subject
if [[ $is_milliseconds == true* ]]
then
        echo  "create table $db_name.$tmp_table_name stored as ORC as select * from $db_name.$orc_table_name where cast($ts_column as bigint) > $two_years_old_ts_ms;" >> /tmp/scripts/remove_dups_$db_name_$subject
else
        if [[ $is_timestamp == true ]]
        then
                echo  "create table $db_name.$tmp_table_name stored as ORC as select * from $db_name.$orc_table_name where cast($ts_column as bigint) > $two_years_old_ts;" >> /tmp/scripts/remove_dups_$db_name_$subject
        else
                echo  "create table $db_name.$tmp_table_name stored as ORC as select * from $db_name.$orc_table_name where $ts_column  > '$two_years_old_date';" >> /tmp/scripts/remove_dups_$db_name_$subject
        fi
fi
echo "drop table if exists $db_name.$tmp1_table_name;" >> /tmp/scripts/remove_dups_$db_name_$subject
if [[ $is_milliseconds == true* ]] || [[ $is_timestamp == true ]]
then
        echo "create table $db_name.$tmp1_table_name stored as ORC as select $pk_columns,max(cast($ts_column as bigint)) as $ts_column from $db_name.$tmp_table_name group by $pk_columns;" >> /tmp/scripts/remove_dups_$db_name_$subject
else
        echo "create table $db_name.$tmp1_table_name stored as ORC as select $pk_columns,max($ts_column ) as $ts_column from $db_name.$tmp_table_name group by $pk_columns;" >> /tmp/scripts/remove_dups_$db_name_$subject
fi
echo "insert overwrite table $db_name.$final_table_name select a.* from $db_name.$tmp_table_name a, $db_name.$tmp1_table_name b where $join_condition;" >> /tmp/scripts/remove_dups_$db_name_$subject
cat  /tmp/scripts/remove_dups_$db_name_$subject
hive_cmd="sudo -u ods_archive $HIVE_HOME/bin/hive  -f /tmp/scripts/remove_dups_$db_name_$subject"
echo $hive_cmd
$dry eval $hive_cmd &
PID2=$!
echo "Hive remove dup form $db_name.$subject PID is $PID2"
$dry wait $PID2
echo "Hive remove dup form $db_name.$subject  finished"
