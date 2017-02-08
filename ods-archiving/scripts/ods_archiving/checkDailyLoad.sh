export PATH=~/airflow-jobs/scripts/ods_archiving/anaconda2/bin:$PATH
#export SPARK_HOME=/usr/hdp/current/spark-client
#export PATH=~/airflow-jobs/scripts/ods_archiving/anaconda2/bin:$PATH:$SPARK_HOME/bin
echo $PATH
db_name=$1
echo $db_name
dq_scripts_dir=/home/airflow/airflow-jobs/scripts/ods_archiving
rerun_scripts_dir=/home/airflow/airflow-jobs/scripts/ods_archiving/rerun/
export PYTHONPATH=$dq_scripts_dir
echo $PYTHONPATH
if [[ $db_name == "cloud_usage_events" ]]
then
        echo "DQ for $db_name"
        python $dq_scripts_dir/quality_control_by_ods_ceu.py $db_name
        y_date=$(date --date yesterday "+%Y-%m-%d")
             r_file="$rerun_scripts_dir$db_name"_"$y_date.sh"
             echo $r_file
             if [[ -f $r_file ]]
             then
                 echo "Rerun file exists will run"
                 m_file="$r_file"_"done"
                 mv $r_file $m_file
                 sh $m_file
             fi

        #$SPARK_HOME/bin/spark-submit --master yarn  --py-files $dq_scripts_dir/ods_quality_control.py,$dq_scripts_dir/quality_control_queries.py,$dq_scripts_dir/constants.py $dq_scripts_dir/quality_control_by_ods_ceu.py $db_name
else
        if [[ $db_name == "usldb_ods" ]]
        then
             #$SPARK_HOME/bin/spark-submit --master yarn  --py-files $dq_scripts_dir/ods_quality_control.py,$dq_scripts_dir/quality_control_queries.py,$dq_scripts_dir/constants.py $dq_scripts_dir/quality_control_by_ods_usl.py $db_name
             python $dq_scripts_dir/quality_control_by_ods_usl.py $db_name
             y_date=$(date --date yesterday "+%Y-%m-%d")
             r_file="$rerun_scripts_dir$db_name"_"$y_date.sh"
             echo $r_file
             if [[ -f $r_file ]]
             then
                 echo "Rerun file exists will run"
                 m_file="$r_file"_"done"
                 mv $r_file $m_file
                 sh $m_file
             fi
        else
             #$SPARK_HOME/bin/spark-submit --master yarn  --py-files $dq_scripts_dir/ods_quality_control.py,$dq_scripts_dir/quality_control_queries.py,$dq_scripts_dir/constants.py $dq_scripts_dir/quality_control_by_ods.py $db_name
             python $dq_scripts_dir/quality_control_by_ods.py $db_name
             y_date=$(date --date yesterday "+%Y-%m-%d")
             r_file="$rerun_scripts_dir$db_name"_"$y_date.sh"
             echo $r_file
             if [[ -f $r_file ]]
             then
                 echo "Rerun file exists will run"
                 m_file="$r_file"_"done"
                 mv $r_file $m_file
                 sh $m_file
             fi

        fi
fi
