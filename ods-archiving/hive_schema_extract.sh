echo $1
rm -f tableNames.txt
rm -f HiveTableDDL.txt
hive -e "use $1; show tables;" > $1_tableNames.txt
wait
echo "CREATE database $1;" >>$1_HiveTableDDL.txt
echo "USE $1;" >> $1_HiveTableDDL.txt
cat $1_tableNames.txt |while read LINE
   do
      hive -e "use $1;show create table $LINE" >>$1_HiveTableDDL.txt
      echo  -e "\n" >> $1_HiveTableDDL.txt
    done
    #rm -f tableNames.txt
echo "Table DDL generated"

