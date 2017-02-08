from pyspark import SparkConf, SparkContext, SQLContext

from pyspark.sql import HiveContext
from pyspark.sql.types import *
import constants as c
import time
import datetime
import os
from operator import itemgetter
from dateutil.relativedelta import relativedelta
conf = SparkConf().setAppName("Supression Intervals")
conf.set("spark.driver.maxResultSize", "3g")
sc = SparkContext(conf = conf)
sqlContext= HiveContext(sc)
ONLINE_STATUS=sc.broadcast([12,15,42,44])
def findsuppression(et):
    supression_start_time=0
    supression_end_time=0
    computer=et[0]


    status_list=sorted(et[1],key=itemgetter(2))
    rset=[()]
    one_status=()

    for one_status in status_list:
        print one_status
        if one_status[1] in ONLINE_STATUS.value:
                if supression_start_time > 0:
                        rset.append((computer, supression_start_time,one_status[2]))
                        supression_start_time=0
        else:
            if supression_start_time == 0:
                 supression_start_time=one_status[2]

    if supression_start_time > 0:
        if one_status[1] in ONLINE_STATUS.value:
                rset.append((computer, supression_start_time,one_status[2]))
        else:
                rset.append((computer, supression_start_time, 253370764800))
    if len(rset) > 0:
        return rset
    else:
        return None
get_data="select computer_number, old_status_number,new_status_number, unix_timestamp(to_utc_timestamp(tranistion_timestamp,'CST')) as tranistion_timestamp from up_time_calculation.server_status_transition_log order by computer_number, tranistion_timestamp"

devStatusRDD=sqlContext.sql(get_data)
devStatusTupleRDD= devStatusRDD.map(lambda (x,y,z,a): (x,[(y,z,a)])).reduceByKey(lambda a,b: a+b).flatMap(findsuppression).filter(lambda x: len(x) > 0)
df = sqlContext.createDataFrame(devStatusTupleRDD, ['device','start_time','end_time'])
df.registerTempTable('s_durations')
sqlContext.sql('use up_time_calculation')

sqlContext.sql('truncate table suppression_intervals')

os.system('hadoop dfs -chmod 777 /apps/hive/warehouse/up_time_calculation.db/suppression_intervals')

insert_stmt='insert into table up_time_calculation.suppression_intervals select device, start_time, end_time from s_durations'
sqlContext.sql(insert_stmt)
