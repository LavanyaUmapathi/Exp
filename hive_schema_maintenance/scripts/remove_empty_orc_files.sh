#!/bin/bash -e
#""" USAGE: ./remove_empty_part_files.sh <qualified hdfs dir path> """
HDFS=$1
export HADOOP_CLIENT_OPTS="-XX:-UseGCOverheadLimit -Xmx4096m"
echo "checking for empty files in $HDFS..."
IFS=$'\n'
rmfiles=()
for i in `hadoop fs -ls $HDFS/* | grep -e "$HDFS/.*" | awk '{print $0}'`; do
  file=$(echo $i | awk '{print $8}')
  size=$(echo $i | awk '{print $5}')
  if [ "$size" -eq "49" ]; then
  #    echo "deleting $file ...."
      echo "will delete $file ..."
      rmfiles+=("$file")
      if [ "${#rmfiles[@]}" -eq "100" ]; then
        echo "deleting ${#rmfiles[@]} files..."
        hdfs dfs -rm -skipTrash ${rmfiles[@]}
        rmfiles=()
      fi
  fi
done
if [ "${#rmfiles[@]}" -gt "0" ]; then
  echo "deleting ${#rmfiles[@]} files..."
  hdfs dfs -rm -skipTrash ${rmfiles[@]}
fi
