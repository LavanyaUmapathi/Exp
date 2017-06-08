#!/bin/sh

HIVE_OPTS="-hiveconf mapred.fairscheduler.pool=misc -hiveconf fs.permissions.umask-mode=022 -hiveconf hive.root.logger=INFO,console -hiveconf derby.stream.error.file=/dev/null -v"

hive $HIVE_OPTS -f update_maas_account_tables.hql
