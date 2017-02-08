# cron common code

export PATH=$ADMIN/bin:$ADMIN/cron:$ADMIN/hadoop-status:/usr/local/sbin:/usr/local/bin:/usr/local/maven-3.0.5/bin:/data/play:/data/scala/sbt-0.13.1/bin:$PATH

if [ -n "$PREFIX" ]; then
    LOGS=$PIPELINE/logs/$PREFIX
    DT=`date -u +%Y-%m-%d`
    LOG="$LOGS/$DT-$PREFIX.log"
    latest_log="$LOGS/latest.log"

    mkdir -p $LOGS

    ln -sf $LOG $latest_log
fi
