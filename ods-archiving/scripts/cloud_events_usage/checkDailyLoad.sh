currDate=$(date   +%Y-%m-%d)
echo "Current date: $currDate"
for file in /var/run/ods_archive/cloud_events_usage/dates/*
do
   cmd="cat /var/run/ods_archive/cloud_events_usage/dates/$file"
   filedate="$($cmd)"
   if [[ $filedate != $currDate ]]
   then
      echo "Backup failed for $file"
      cmd="echo 'Hadoop Backup failed for $file. Next backup date is $filedate' | mail -s 'Hadoop backup failed' GET_HADOOP_SUPPORT@rackspace.com"
      eval $cmd
   fi
done
cmd="echo 'Hadoop Backup for cloud_events_usage finished.' | mail -s 'Hadoop backup completed for $db_name' GET_HADOOP_SUPPORT@rackspace.com"
eval $cmd

