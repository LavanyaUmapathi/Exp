usage="$(basename "$0") db_name table_name index_column schema_name populate_orc_table_flag isDate isTimestamp"
if [[ $1 == "-h" ]]
then
  echo $usage
  echo "The script loads usldb_ods tables from Microsoft SQL Server to Hadoop V2"
  echo "Dry run available as:  $(basename "$0") -dry"
  exit
fi
dry=
if [[ $1 == "-dry" ]]
then
   dry="echo"
fi

set -e
export SQOOP_HOME=/usr/hdp/current/sqoop-client
export HIVE_HOME=/usr/hdp/current/hive-client
export HADOOP_HOME=/usr/hdp/current/hadoop-client
export AIRFLOW_RUN_DIR=/home/airflow/airflow-jobs/scripts
export RUN_DIR=/var/run/ods_archive
db_name=$1
subject=$2
dateColumnName=$3
schema=$4
orcFlag=$5
isDate=$6
isTimestamp=$7

last_id=0
if [[ $dry == "echo" ]]
then
  db_name=$2
  subject=$3
  dateColumnName=$4
  schema=$5
  orcFlag=$6
  isDate=$7
  isTimestamp=$8
fi
sql_ip=$(head -n 1 $RUN_DIR/.sqlconn)
sql_u_name=$(head -n 2 $RUN_DIR/.sqlconn | tail -n 1)
sql_p_word=$(tail -n 1 $RUN_DIR/.sqlconn)
echo $sql_ip
echo $sql_u_name
echo $sql_p_word
orc="_orc"
subject_orc=$subject$orc
echo $subject
echo $dateColumnName
echo $subject_orc
if [[ -e $RUN_DIR/$db_name/dates/$subject ]]
then
   echo "Date file exists"
else
   echo "Create start date file!"
   exit 1
fi
set -e
if [[ $dry == "echo" ]]
then
  echo "Set start date in $RUN_DIR/$db_name/dates/$subject file"
fi
echo "Check if it is ORC load"
if [[ ( $orcFlag == true ) && ( -e /tmp/scripts/$subject ) ]]
then
        echo "ORC flag is set verify that previous load has completed"
        lline=$(awk 'END{gsub(/\n/,"",$0); print $0}' /tmp/scripts/$subject)
        echo "Last line is $lline"
        if [[ $lline == "completed" ]]
        then
            echo "Previous ORC load completed sucessfully"
        else
            echo "Previous ORC load did not compete sucessfully"
            if [[ $lline == "exit;" ]]
            then
                echo "Previous ORC load started but it did not complete, aborting current run!"
                cmd="echo 'ORC table load failed for $subject. Please investigate' | mail -s 'ORC load failed for $subject ' GET_HADOOP_SUPPORT@rackspace.com"
                eval $cmd
                exit
            else
                echo "Previous ORC load did not start. Executed what has been collected in the previous run!"
                hiveORCCmd="sudo -u ods_archive $HIVE_HOME/bin/hive  -f /tmp/scripts/$subject"
                echo $hiveORCCmd
                $dry eval $hiveORCCmd  &
                PID2=$!
                echo "Hive load orc PID is $PID2"
                $dry wait $PID2
                echo "Hive load orc finished"
                echo "completed"  >> /tmp/scripts/$subject
            fi
        fi

fi
if [[ $orcFlag == true ]]
then
        $dry echo "ADD JAR  hdfs:/hdp/apps/2.2.4.2-2/hive/auxjars/csv-serde-1.1.2-0.11.0-all.jar;" > /tmp/scripts/$subject
        $dry echo "set hive.stats.autogather=false;" >> /tmp/scripts/$subject
        $dry echo "set hive.support.quoted.identifiers=column;" >> /tmp/scripts/$subject
fi
cdate=$(awk '{print $1}' $RUN_DIR/$db_name/dates/$subject)
cdatets=''
ndate=''
ndatets=''
if [[ $isDate == true ]]
then
        cdatets=$(date -d "$cdate " +%s)
        ndate=$(date -d "$cdate +1day" +%Y-%m-%d)
        ndatets=$(date -d "$ndate " +%s)
fi
targetdir="/apps/hive/warehouse/$db_name.db/$subject"
echo $cdate
echo $targetdir
echo $cdatets
echo "****************"

currDate=$(date  +%Y-%m-%d)
currdatets=$(date -d "$currDate " +%s)
echo $currDate

if [[ $dry == "echo" ]]
then
   echo "The following will be performed up to the current date"
fi




echo "started to load"
newdir="$targetdir"
echo $newdir
echo $isDate
if [[ $isDate == true ]]
then
        while [[ ("$cdate" != "$currDate") && ("$cdate" < "$currDate") ]]
        do
                if [[ $dry == "echo" ]]
                then
                        echo "When processControlFlag file contains value 1 the process will exit"
                fi


                stopProcess=$(awk '{print $1}' $RUN_DIR/$db_name/processControlFlag)
                if [[ "$stopProcess" == 1 ]]
                then
                        echo "Received signal to stop processing $subject"
                        break
                fi

                if [[ $isTimestamp == true* ]]
                then
                        sqoopcmd="sudo -u ods_archive $SQOOP_HOME/bin/sqoop import --connect \"jdbc:sqlserver://$sql_ip;database=$db_name;username=$sql_u_name;password=$sql_p_word\"  --table $subject --where \"$dateColumnName>=$cdatets and $dateColumnName<$ndatets\" --fields-terminated-by , --escaped-by \\\ --enclosed-by '\"'    --compress -m 1 --target-dir $targetdir --append --hive-drop-import-delims -- --schema $schema --table-hints NOLOCK"
                else
                        sqoopcmd="sudo -u ods_archive $SQOOP_HOME/bin/sqoop import --connect \"jdbc:sqlserver://$sql_ip;database=$db_name;username=$sql_u_name;password=$sql_p_word\"  --table $subject --where \"$dateColumnName>='$cdate' and $dateColumnName<'$ndate'\" --fields-terminated-by , --escaped-by \\\ --enclosed-by '\"'    --compress -m 1 --target-dir $targetdir --append --hive-drop-import-delims -- --schema $schema --table-hints NOLOCK"
                fi
                echo $sqoopcmd
                $dry eval $sqoopcmd
                if [[ $dry == "echo" ]]
                then
                        echo "Exiting dry run"
                        exit
                fi
                echo $currDate > $RUN_DIR/$db_name/dates/$subject
                if [[ $orcFlag == "true" ]]
                then
                        if [[ $isTimestamp == true* ]]
                        then
                                echo "insert into table $db_name.$subject$orc select * from $db_name.$subject where $dateColumnName >= $cdatets and $dateColumnName < $ndatets;" >> /tmp/scripts/$subject
                        else
                                echo "insert into table $db_name.$subject$orc select * from $db_name.$subject where $dateColumnName >= $cdate and $dateColumnName < $ndate;" >> /tmp/scripts/$subject
                        fi
                fi
                echo "cmpleted load $cdate"
                cdate=$(date -d "$cdate +1day" +%Y-%m-%d)
                ndate=$(date -d "$ndate +1day" +%Y-%m-%d)
                echo "new cdate $cdate; new ndate $ndate"
                cdatets=$(date -d "$cdate " +%s)
                ndatets=$(date -d "$ndate " +%s)

        done
        echo "All dates are already processed for $subject"
else
       echo "Starting param is not date selecting all records where selected id is greated than last collected id"
       GLOBIGNORE="*"
       stop_loading=false
       while [[ $stop_loading == false ]]
       do
                stopProcess=$(awk '{print $1}' $RUN_DIR/$db_name/processControlFlag)
                if [[ "$stopProcess" == 1 ]]
                then
                        echo "Received signal to stop processing $subject"
                        break
                fi



                sqoopcmd="sudo -u ods_archive $SQOOP_HOME/bin/sqoop import --connect \"jdbc:sqlserver://$sql_ip;database=$db_name;username=$sql_u_name;password=$sql_p_word\"  --query 'select TOP 10000000 *  from $subject WITH (NOLOCK) where $dateColumnName > $cdate and \$CONDITIONS order by $dateColumnName asc' --fields-terminated-by , --escaped-by \\\ --enclosed-by '\"'    --compress -m 1 --target-dir $targetdir --check-column $dateColumnName --incremental append --last-value $cdate --hive-drop-import-delims"
                echo $sqoopcmd
                load_output=$($dry eval $sqoopcmd)
                echo $load_output
                if [[ $dry == "echo" ]]
                then
                        echo "Exiting dry run"
                        exit
                fi
                loaded_r_count=$(echo  $load_output|awk '{for(i=1;i<=NF;i++) if ($i=="Retrieved") print $(i+1)}')
                last_id=$( echo  $load_output|awk '{for(i=1;i<=NF;i++) if ($i=="--last-value") print $(i+1)}')
                if [[ $last_id != "null" ]]
                then
                        echo  $last_id >  $RUN_DIR/$db_name/dates/$subject
                        echo "Last Id is:" $last_id
                        echo "Loaded record count is :" $loaded_r_count

                        if [[ $orcFlag == "true" ]]
                        then
                                echo "insert into  table $db_name.$subject$orc select * from $db_name.$subject where cast($dateColumnName as bigint)  > $cdate and cast($dateColumnName as bigint) <= $last_id;" >> /tmp/scripts/$subject
                        fi

                        if (( $loaded_r_count < 10000000 ))
                        then
                                stop_loading=true
                        else
                                cdate=$last_id
                        fi
                else
                        echo "No Data - NULL Value returned"
                        stop_loading=true
                fi
        done
        echo "exit;" >> /tmp/scripts/$subject
        cat /tmp/scripts/$subject
        chmod 755 /tmp/scripts/$subject
        hiveORCCmd="sudo -u ods_archive $HIVE_HOME/bin/hive  -f /tmp/scripts/$subject"
        echo $hiveORCCmd
        $dry eval $hiveORCCmd  &
        PID2=$!
        echo "Hive load orc PID is $PID2"
        $dry wait $PID2
        echo "Hive load orc finished"
        echo "completed"  >> /tmp/scripts/$subject
fi
