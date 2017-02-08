db_name=$1
echo $db_name

currDate=$(date   +%Y-%m-%d)
echo "Current date: $currDate"
if [[ -e /home/airflow/airflow-jobs/scripts/$db_name/dates/ ]]
then
for file in /home/airflow/airflow-jobs/scripts/$db_name/dates/*
do
   cmd="cat $file"
   echo $cmd
   filedate="$($cmd)"
   echo $filedate
   if [[ $filedate != $currDate ]]
   then
      echo "Backup failed for $file"
      cmd="echo 'Hadoop Backup failed for $file. Next backup date is $filedate' | mail -s 'Hadoop backup failed' GET_HADOOP_SUPPORT@rackspace.com"
      eval $cmd
   fi
done
fi
cmd="echo 'Hadoop Backup for $db_name finished.' | mail -s 'Hadoop backup completed for $db_name' GET_HADOOP_SUPPORT@rackspace.com"
eval $cmd
