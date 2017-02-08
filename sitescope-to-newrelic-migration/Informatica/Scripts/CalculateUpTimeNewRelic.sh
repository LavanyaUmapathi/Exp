sleep 120
export JAVA_HOME=/home/informatica/cassandraV2/jdk1.7.0_80
export CASSANDRA_HOME=/home/informatica/cassandraV2/apache-cassandra-2.1.8
export CASSANDRA_CONF=/home/informatica/cassandraV2/apache-cassandra-2.1.8/conf
export CASSANDRA_BULK_LOAD_DIR=/mnt/ebi/acg
$JAVA_HOME/bin/java -Xms5G -Xmx5G -cp /home/informatica/cassandraV2/lib/up-time-calculation-v2-1.0.0-SNAPSHOT-jar-with-dependencies.jar   com.rackspace.foundation.up.time.calculation.UpTimeCalculationExec incremental true NewRelic   EBIIndex0 10.12.250.235 csv > /dev/null 2>&1
#cp $CASSANDRA_BULK_LOAD_DIR/csv/*.csv /mnt/ebi/acg/cassandra_csv/UpTimeCalcData/UpTimeCalculationSuper/hadoop/.
