#!/bin/bash
#
# Load the clickstream data for a given DATE
#
# USAGE: clickstream-load-date.sh [OPTIONS] YYYY-MM-DD [YYYY-MM-DD]
# -n/--dryrun : show but do not execute
#
# If two dates are given they describe an inclusive range
#


if test -z "$ADMIN"; then
    ADMIN=/data/acumen-admin
fi
. $ADMIN/cron/config.sh


program=`basename $0`

# File system
raw_data_file_list="/tmp/${program}-$$-clickstream-data-files.lst"
input_data_file_list="/tmp/${program}-$$-input-data-files.lst"
hql_script_file="/tmp/${program}-$$-script.hql"
db_files_list="/tmp/${program}-$$-db-files.lst"
hive_out_file="/tmp/${program}-$$-hive-out.log"
hive_err_file="/tmp/${program}-$$-hive-err.log"

# HDFS config
HDFS_WORKING_DIR="/tmp/${program}-$$"
HDFS_DATA_INPUT_DIR="$HDFS_CLICKSTREAM_INPUT_DIR"

# Hive config
HIVE_DATABASE="$HIVE_CLICKSTREAM_DATABASE"
HIVE_TABLE="$HIVE_CLICKSTREAM_TABLE"
HIVE_PROD_TABLE="$HIVE_TABLE"
HIVE_STAGING_TABLE="${HIVE_TABLE}_stg"
HIVE_PROD_TABLE_DIR="$HIVE_HDFS_ROOT/${HIVE_CLICKSTREAM_DATABASE}.db/${HIVE_TABLE}"


######################################################################
# Nothing below here should need changing

. $ADMIN/cron/run.sh

usage() {
  echo "Usage: $program [OPTIONS] START-DATE [END-DATE]" 1>&2
  echo "  dates are in format YYYY-MM-DD" 1>&2
  echo "OPTIONS:" 1>&2
  echo "  -h             Show this help message" 1>&2
  echo "  -n / --dryrun  Show but do not execute commands" 1>&2
}


dryrun=0
while getopts "hn" o ; do
  case "${o}" in
    h)
     usage
     exit 0
     ;;
    n)
     dryrun=1
     ;;
    \?)
      echo "$program: ERROR: Unknown option -$OPTARG" 1>&2
      echo "$program: Use $program -h for usage" 1>&2
      exit 1
      ;;
   esac
done

shift $((OPTIND-1))

if [ $# -lt 1 -o $# -gt 2 ]; then
  echo "$program: ERROR: expected 1 or 2 arguments" 1>&2
  usage
  exit 1
fi

start_date=$1
if [ $# -gt 1 ] ; then
  end_date=$2
else
  end_date=$start_date
fi

if test $start_date '>' $end_date; then
  echo "$program: ERROR: Start date $start_date is after end date $end_date" 1>&2;
  exit 1
fi

if test $dryrun = 1; then
  echo "$program: Running in DRYRUN mode - no executing" 1>&2
fi


trap "rm -f $raw_data_file_list $input_data_file_list $hql_script_file $db_files_list $hive_out_file $hive_err_file" 0 9 15



today=`date -u +%Y-%m-%d`

echo "$program: Loading data $start_date to $end_date inclusive" 1>&2

if test $start_date = $today -o $start_date '<' $today; then
  if test $today = $end_date -o $today '<' $end_date; then
    echo "$program: ERROR: today is in range $start_date to $end_date - ending" 1>&2
    exit 1
  fi
fi


echo "$program: Looking for input files in $HDFS_DATA_INPUT_DIR:" 1>&2
echo "  $start_date ... $end_date: all files" 1>&2

start_date_nom=`echo $start_date | tr -d '-'`
end_date_nom=`echo $end_date | tr -d '-'`

# Filenames like: hit_data_20150210_230000.csv.gz
#
# Output file format: <date-from-path> \t <full-hdfs-path> \t <file-name>
hadoop fs -ls $HDFS_DATA_INPUT_DIR | \
  awk '!/^Found/ { print $8 }' | \
  sed -e 's,^\(.*/\)\(hit_data_\)\([0-9]*\)\(.*\),\3\t\1\2\3\4\t\2\3\4,' \
  > $raw_data_file_list

awk "{if(\$1 >= \"${start_date_nom}\" && \$1 <= \"${end_date_nom}\") { print \$2 } }" < $raw_data_file_list  >> $input_data_file_list

input_files_count=`wc -l < $input_data_file_list`

space_separated_paths=`tr '\012' ' ' < $input_data_file_list`

if test $input_files_count -eq 0; then
  echo "$program: No data available for date range $start_date ... $end_date" 1>&2
  exit 1
fi

echo "$program: Found $input_files_count files for date range $start_date ... $end_date" 1>&2
if test $dryrun = 1; then
  echo "$program: Would process these input files" 1>&2
  sed -e 's/^/    /' $input_data_file_list  1>&2
fi

# Delete working space
cmd="hadoop fs -rmr $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  $cmd && true
fi

# Copy input files to working space so they can be loaded/moved into hive
cmd="hadoop fs -mkdir $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  $cmd && true
fi
cmd="hadoop fs -cp $space_separated_paths $HDFS_WORKING_DIR/"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  $cmd
fi

# Check the copy outputs
file_glob="$HDFS_WORKING_DIR/*"

hadoop fs -ls $file_glob 2>/dev/null | sed -e 's/^/    /' > $db_files_list
db_files_list_count=`wc -l < $db_files_list`

# Load the CSV files in the Hive DB
echo "$program: Processing loading $input_files_count files into $HIVE_TABLE" 1>&2
echo "     Source: $file_glob" 1>&2
  sed -e 's/^/    /' $db_files_list  1>&2
echo "    Staging: Hive table $HIVE_STAGING_TABLE" 1>&2
echo "       Dest: Hive table $HIVE_PROD_TABLE" 1>&2


# Build HQL to create staging table and load ETL output files into it
cat > $hql_script_file <<EOF
USE ${HIVE_DATABASE};

ADD JAR ${CSV_SERDE_JAR};

DROP TABLE IF EXISTS ${HIVE_STAGING_TABLE};

CREATE TABLE ${HIVE_STAGING_TABLE}
(
  accept_language STRING,
  browser STRING,
  browser_height STRING,
  browser_width STRING,
  campaign STRING,
  c_color STRING,
  channel STRING,
  click_action STRING,
  click_action_type STRING,
  click_context STRING,
  click_context_type STRING,
  click_sourceid STRING,
  click_tag STRING,
  code_ver STRING,
  color STRING,
  connection_type STRING,
  cookies STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  country STRING,
  ct_connect_type STRING,
  currency STRING,
  curr_factor STRING,
  curr_rate STRING,
  cust_hit_time_gmt STRING,
  cust_visid STRING,
  daily_visitor STRING,
  date_time STRING COMMENT 'May have to change to STRING if not in the yyyy-mm-dd hh:mm:ss[.f...] format',
  domain STRING,
  duplicated_from STRING,
  duplicate_events STRING,
  duplicate_purchase STRING,
  evar1 STRING,
  evar2 STRING,
  evar3 STRING,
  evar4 STRING,
  evar5 STRING,
  evar6 STRING,
  evar7 STRING,
  evar8 STRING,
  evar9 STRING,
  evar10 STRING,
  evar11 STRING,
  evar12 STRING,
  evar13 STRING,
  evar14 STRING,
  evar15 STRING,
  evar16 STRING,
  evar17 STRING,
  evar18 STRING,
  evar19 STRING,
  evar20 STRING,
  evar21 STRING,
  evar22 STRING,
  evar23 STRING,
  evar24 STRING,
  evar25 STRING,
  evar26 STRING,
  evar27 STRING,
  evar28 STRING,
  evar29 STRING,
  evar30 STRING,
  evar31 STRING,
  evar32 STRING,
  evar33 STRING,
  evar34 STRING,
  evar35 STRING,
  evar36 STRING,
  evar37 STRING,
  evar38 STRING,
  evar39 STRING,
  evar40 STRING,
  evar41 STRING,
  evar42 STRING,
  evar43 STRING,
  evar44 STRING,
  evar45 STRING,
  evar46 STRING,
  evar47 STRING,
  evar48 STRING,
  evar49 STRING,
  evar50 STRING,
  evar51 STRING,
  evar52 STRING,
  evar53 STRING,
  evar54 STRING,
  evar55 STRING,
  evar56 STRING,
  evar57 STRING,
  evar58 STRING,
  evar59 STRING,
  evar60 STRING,
  evar61 STRING,
  evar62 STRING,
  evar63 STRING,
  evar64 STRING,
  evar65 STRING,
  evar66 STRING,
  evar67 STRING,
  evar68 STRING,
  evar69 STRING,
  evar70 STRING,
  evar71 STRING,
  evar72 STRING,
  evar73 STRING,
  evar74 STRING,
  evar75 STRING,
  event_list STRING,
  exclude_hit STRING,
  first_hit_pagename STRING,
  first_hit_page_url STRING,
  first_hit_referrer STRING,
  first_hit_time_gmt STRING,
  geo_city STRING,
  geo_country STRING,
  geo_dma STRING,
  geo_region STRING,
  geo_zip STRING,
  hier1 STRING,
  hier2 STRING,
  hier3 STRING,
  hier4 STRING,
  hier5 STRING,
  hitid_high STRING,
  hitid_low STRING,
  hit_source STRING,
  hit_time_gmt STRING,
  homepage STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  hourly_visitor STRING,
  ip STRING,
  ip2 STRING,
  java_enabled STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  javascript STRING,
  j_jscript STRING,
  language STRING,
  last_hit_time_gmt STRING,
  last_purchase_num STRING,
  last_purchase_time_gmt STRING,
  mobile_id STRING,
  monthly_visitor STRING,
  mvvar1 STRING,
  mvvar2 STRING,
  mvvar3 STRING,
  namespace STRING,
  new_visit STRING,
  os STRING,
  page_event STRING,
  page_event_var1 STRING,
  page_event_var2 STRING,
  page_event_var3 STRING,
  pagename STRING,
  page_type STRING,
  page_url STRING,
  paid_search STRING,
  partner_plugins STRING,
  persistent_cookie STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  plugins STRING,
  post_browser_height STRING,
  post_browser_width STRING,
  post_campaign STRING,
  post_channel STRING,
  post_cookies STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  post_currency STRING,
  post_cust_hit_time_gmt STRING,
  post_cust_visid STRING,
  post_evar1 STRING,
  post_evar2 STRING,
  post_evar3 STRING,
  post_evar4 STRING,
  post_evar5 STRING,
  post_evar6 STRING,
  post_evar7 STRING,
  post_evar8 STRING,
  post_evar9 STRING,
  post_evar10 STRING,
  post_evar11 STRING,
  post_evar12 STRING,
  post_evar13 STRING,
  post_evar14 STRING,
  post_evar15 STRING,
  post_evar16 STRING,
  post_evar17 STRING,
  post_evar18 STRING,
  post_evar19 STRING,
  post_evar20 STRING,
  post_evar21 STRING,
  post_evar22 STRING,
  post_evar23 STRING,
  post_evar24 STRING,
  post_evar25 STRING,
  post_evar26 STRING,
  post_evar27 STRING,
  post_evar28 STRING,
  post_evar29 STRING,
  post_evar30 STRING,
  post_evar31 STRING,
  post_evar32 STRING,
  post_evar33 STRING,
  post_evar34 STRING,
  post_evar35 STRING,
  post_evar36 STRING,
  post_evar37 STRING,
  post_evar38 STRING,
  post_evar39 STRING,
  post_evar40 STRING,
  post_evar41 STRING,
  post_evar42 STRING,
  post_evar43 STRING,
  post_evar44 STRING,
  post_evar45 STRING,
  post_evar46 STRING,
  post_evar47 STRING,
  post_evar48 STRING,
  post_evar49 STRING,
  post_evar50 STRING,
  post_evar51 STRING,
  post_evar52 STRING,
  post_evar53 STRING,
  post_evar54 STRING,
  post_evar55 STRING,
  post_evar56 STRING,
  post_evar57 STRING,
  post_evar58 STRING,
  post_evar59 STRING,
  post_evar60 STRING,
  post_evar61 STRING,
  post_evar62 STRING,
  post_evar63 STRING,
  post_evar64 STRING,
  post_evar65 STRING,
  post_evar66 STRING,
  post_evar67 STRING,
  post_evar68 STRING,
  post_evar69 STRING,
  post_evar70 STRING,
  post_evar71 STRING,
  post_evar72 STRING,
  post_evar73 STRING,
  post_evar74 STRING,
  post_evar75 STRING,
  post_event_list STRING,
  post_hier1 STRING,
  post_hier2 STRING,
  post_hier3 STRING,
  post_hier4 STRING,
  post_hier5 STRING,
  post_java_enabled STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  post_keywords STRING,
  post_mvvar1 STRING,
  post_mvvar2 STRING,
  post_mvvar3 STRING,
  post_page_event STRING,
  post_page_event_var1 STRING,
  post_page_event_var2 STRING,
  post_page_event_var3 STRING,
  post_pagename STRING,
  post_pagename_no_url STRING,
  post_page_type STRING,
  post_page_url STRING,
  post_partner_plugins STRING,
  post_persistent_cookie STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  post_product_list STRING,
  post_prop1 STRING,
  post_prop2 STRING,
  post_prop3 STRING,
  post_prop4 STRING,
  post_prop5 STRING,
  post_prop6 STRING,
  post_prop7 STRING,
  post_prop8 STRING,
  post_prop9 STRING,
  post_prop10 STRING,
  post_prop11 STRING,
  post_prop12 STRING,
  post_prop13 STRING,
  post_prop14 STRING,
  post_prop15 STRING,
  post_prop16 STRING,
  post_prop17 STRING,
  post_prop18 STRING,
  post_prop19 STRING,
  post_prop20 STRING,
  post_prop21 STRING,
  post_prop22 STRING,
  post_prop23 STRING,
  post_prop24 STRING,
  post_prop25 STRING,
  post_prop26 STRING,
  post_prop27 STRING,
  post_prop28 STRING,
  post_prop29 STRING,
  post_prop30 STRING,
  post_prop31 STRING,
  post_prop32 STRING,
  post_prop33 STRING,
  post_prop34 STRING,
  post_prop35 STRING,
  post_prop36 STRING,
  post_prop37 STRING,
  post_prop38 STRING,
  post_prop39 STRING,
  post_prop40 STRING,
  post_prop41 STRING,
  post_prop42 STRING,
  post_prop43 STRING,
  post_prop44 STRING,
  post_prop45 STRING,
  post_prop46 STRING,
  post_prop47 STRING,
  post_prop48 STRING,
  post_prop49 STRING,
  post_prop50 STRING,
  post_prop51 STRING,
  post_prop52 STRING,
  post_prop53 STRING,
  post_prop54 STRING,
  post_prop55 STRING,
  post_prop56 STRING,
  post_prop57 STRING,
  post_prop58 STRING,
  post_prop59 STRING,
  post_prop60 STRING,
  post_prop61 STRING,
  post_prop62 STRING,
  post_prop63 STRING,
  post_prop64 STRING,
  post_prop65 STRING,
  post_prop66 STRING,
  post_prop67 STRING,
  post_prop68 STRING,
  post_prop69 STRING,
  post_prop70 STRING,
  post_prop71 STRING,
  post_prop72 STRING,
  post_prop73 STRING,
  post_prop74 STRING,
  post_prop75 STRING,
  post_purchaseid STRING,
  post_referrer STRING,
  post_search_engine STRING,
  post_state STRING,
  post_survey STRING,
  post_tnt STRING,
  post_transactionid STRING,
  post_t_time_info STRING,
  post_visid_high STRING,
  post_visid_low STRING,
  post_visid_type STRING,
  post_zip STRING,
  p_plugins STRING,
  prev_page STRING,
  product_list STRING,
  product_merchandising STRING,
  prop1 STRING,
  prop2 STRING,
  prop3 STRING,
  prop4 STRING,
  prop5 STRING,
  prop6 STRING,
  prop7 STRING,
  prop8 STRING,
  prop9 STRING,
  prop10 STRING,
  prop11 STRING,
  prop12 STRING,
  prop13 STRING,
  prop14 STRING,
  prop15 STRING,
  prop16 STRING,
  prop17 STRING,
  prop18 STRING,
  prop19 STRING,
  prop20 STRING,
  prop21 STRING,
  prop22 STRING,
  prop23 STRING,
  prop24 STRING,
  prop25 STRING,
  prop26 STRING,
  prop27 STRING,
  prop28 STRING,
  prop29 STRING,
  prop30 STRING,
  prop31 STRING,
  prop32 STRING,
  prop33 STRING,
  prop34 STRING,
  prop35 STRING,
  prop36 STRING,
  prop37 STRING,
  prop38 STRING,
  prop39 STRING,
  prop40 STRING,
  prop41 STRING,
  prop42 STRING,
  prop43 STRING,
  prop44 STRING,
  prop45 STRING,
  prop46 STRING,
  prop47 STRING,
  prop48 STRING,
  prop49 STRING,
  prop50 STRING,
  prop51 STRING,
  prop52 STRING,
  prop53 STRING,
  prop54 STRING,
  prop55 STRING,
  prop56 STRING,
  prop57 STRING,
  prop58 STRING,
  prop59 STRING,
  prop60 STRING,
  prop61 STRING,
  prop62 STRING,
  prop63 STRING,
  prop64 STRING,
  prop65 STRING,
  prop66 STRING,
  prop67 STRING,
  prop68 STRING,
  prop69 STRING,
  prop70 STRING,
  prop71 STRING,
  prop72 STRING,
  prop73 STRING,
  prop74 STRING,
  prop75 STRING,
  purchaseid STRING,
  quarterly_visitor STRING,
  ref_domain STRING,
  referrer STRING,
  ref_type STRING,
  resolution STRING,
  sampled_hit STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  search_engine STRING,
  search_page_num STRING,
  secondary_hit STRING,
  service STRING COMMENT 'Can be changed to VARCHAR(2) once in Hive 0.12',
  sourceid STRING,
  s_resolution STRING,
  state STRING,
  stats_server STRING,
  tnt STRING,
  tnt_post_vista STRING,
  transactionid STRING,
  truncated_hit STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  t_time_info STRING,
  ua_color STRING,
  ua_os STRING,
  ua_pixels STRING,
  user_agent STRING,
  user_hash STRING,
  userid STRING,
  username STRING,
  user_server STRING,
  va_closer_detail STRING,
  va_closer_id STRING,
  va_finder_detail STRING,
  va_finder_id STRING,
  va_instance_event STRING,
  va_new_engagement STRING,
  visid_high STRING,
  visid_low STRING,
  visid_new STRING COMMENT 'Can be changed to VARCHAR(1) once in Hive 0.12',
  visid_timestamp STRING,
  visid_type STRING,
  visit_keywords STRING,
  visit_num STRING,
  visit_page_num STRING,
  visit_referrer STRING,
  visit_search_engine STRING,
  visit_start_pagename STRING,
  visit_start_page_url STRING,
  visit_start_time_gmt STRING,
  weekly_visitor STRING,
  yearly_visitor STRING,
  zip STRING,
  mobileaction STRING,
  mobileappid STRING,
  mobilecampaigncontent STRING,
  mobilecampaignmedium STRING,
  mobilecampaignname STRING,
  mobilecampaignsource STRING,
  mobilecampaignterm STRING,
  mobiledayofweek STRING,
  mobiledayssincefirstuse STRING,
  mobiledayssincelastuse STRING,
  mobiledevice STRING,
  mobilehourofday STRING,
  mobileinstalldate STRING,
  mobilelaunchnumber STRING,
  mobileltv STRING,
  mobileosversion STRING,
  mobileresolution STRING,
  pointofinterest STRING,
  pointofinterestdistance STRING,
  post_mobileaction STRING,
  post_mobileappid STRING,
  post_mobilecampaigncontent STRING,
  post_mobilecampaignmedium STRING,
  post_mobilecampaignname STRING,
  post_mobilecampaignsource STRING,
  post_mobilecampaignterm STRING,
  post_mobiledayofweek STRING,
  post_mobiledayssincefirstuse STRING,
  post_mobiledayssincelastuse STRING,
  post_mobiledevice STRING,
  post_mobilehourofday STRING,
  post_mobileinstalldate STRING,
  post_mobilelaunchnumber STRING,
  post_mobileltv STRING,
  post_mobileosversion STRING,
  post_mobileresolution STRING,
  post_pointofinterest STRING,
  post_pointofinterestdistance STRING,
  socialassettrackingcode STRING,
  socialauthor STRING,
  socialaveragesentiment STRING,
  socialcontentprovider STRING,
  sociallanguage STRING,
  sociallatlong STRING,
  sociallink STRING,
  socialproperty STRING,
  socialterm STRING,
  socialtermslist STRING,
  post_socialassettrackingcode STRING,
  post_socialauthor STRING,
  post_socialaveragesentiment STRING,
  post_socialcontentprovider STRING,
  post_sociallanguage STRING,
  post_sociallatlong STRING,
  post_sociallink STRING,
  post_socialproperty STRING,
  post_socialterm STRING,
  post_socialtermslist STRING,
  video STRING,
  videoad STRING,
  videoadinpod STRING,
  videoadplayername STRING,
  videoadpod STRING,
  videochannel STRING,
  videocontenttype STRING,
  videopath STRING,
  videoplayername STRING,
  videosegment STRING,
  post_video STRING,
  post_videoad STRING,
  post_videoadinpod STRING,
  post_videoadplayername STRING,
  post_videoadpod STRING,
  post_videochannel STRING,
  post_videocontenttype STRING,
  post_videopath STRING,
  post_videoplayername STRING,
  post_videosegment STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;

LOAD DATA INPATH '${file_glob}' INTO TABLE ${HIVE_STAGING_TABLE};

EOF

echo "$program: Running load to staging hive HQL from $hql_script_file" 1>&2
cmd="hive $HIVE_OPTS -f $hql_script_file"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd :" 1>&2
  cat $hql_script_file
else
  echo "$program: running $cmd"
  $cmd 2>&1 | tee $hive_out_file
  status=$?
  sed -e 's/^/    /' $hive_out_file 1>&2
  if test $status != 0; then
    echo "$program: FAILED load to staging hive $cmd with result $status" 1>&2
    exit $status
  fi
fi


# Build HQL to load production table from staging
cat > $hql_script_file <<EOF
USE ${HIVE_DATABASE};

ADD JAR ${CSV_SERDE_JAR};

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.compress.output=true;
SET hive.exec.max.dynamic.partitions=2000;
SET mapred.max.split.size=521000000;
SET mapred.output.compression.type=BLOCK;
SET io.sort.mb=256;
SET io.sort.factor=100;
SET mapred.job.reuse.jvm.num.tasks=-1;
SET hive.enforce.sorting=true;
SET mapreduce.reduce.input.limit = -1;
SET hive.merge.mapredfiles = true;
SET mapred.job.reduce.memory.mb=4096;
SET mapred.job.map.memory.mb=4096;

FROM ${HIVE_STAGING_TABLE} stg
INSERT OVERWRITE TABLE ${HIVE_PROD_TABLE} PARTITION(dt)
 SELECT
      stg.accept_language,
      stg.browser,
      stg.browser_height,
      stg.browser_width,
      stg.campaign,
      stg.c_color,
      stg.channel,
      stg.click_action,
      stg.click_action_type,
      stg.click_context,
      stg.click_context_type,
      stg.click_sourceid,
      stg.click_tag,
      stg.code_ver,
      stg.color,
      stg.connection_type,
      stg.cookies,
      stg.country,
      stg.ct_connect_type,
      stg.currency,
      stg.curr_factor,
      stg.curr_rate,
      stg.cust_hit_time_gmt,
      stg.cust_visid,
      stg.daily_visitor,
      stg.date_time,
      stg.domain,
      stg.duplicated_from,
      stg.duplicate_events,
      stg.duplicate_purchase,
      stg.evar1,
      stg.evar2,
      stg.evar3,
      stg.evar4,
      stg.evar5,
      stg.evar6,
      stg.evar7,
      stg.evar8,
      stg.evar9,
      stg.evar10,
      stg.evar11,
      stg.evar12,
      stg.evar13,
      stg.evar14,
      stg.evar15,
      stg.evar16,
      stg.evar17,
      stg.evar18,
      stg.evar19,
      stg.evar20,
      stg.evar21,
      stg.evar22,
      stg.evar23,
      stg.evar24,
      stg.evar25,
      stg.evar26,
      stg.evar27,
      stg.evar28,
      stg.evar29,
      stg.evar30,
      stg.evar31,
      stg.evar32,
      stg.evar33,
      stg.evar34,
      stg.evar35,
      stg.evar36,
      stg.evar37,
      stg.evar38,
      stg.evar39,
      stg.evar40,
      stg.evar41,
      stg.evar42,
      stg.evar43,
      stg.evar44,
      stg.evar45,
      stg.evar46,
      stg.evar47,
      stg.evar48,
      stg.evar49,
      stg.evar50,
      stg.evar51,
      stg.evar52,
      stg.evar53,
      stg.evar54,
      stg.evar55,
      stg.evar56,
      stg.evar57,
      stg.evar58,
      stg.evar59,
      stg.evar60,
      stg.evar61,
      stg.evar62,
      stg.evar63,
      stg.evar64,
      stg.evar65,
      stg.evar66,
      stg.evar67,
      stg.evar68,
      stg.evar69,
      stg.evar70,
      stg.evar71,
      stg.evar72,
      stg.evar73,
      stg.evar74,
      stg.evar75,
      stg.event_list,
      stg.exclude_hit,
      stg.first_hit_pagename,
      stg.first_hit_page_url,
      stg.first_hit_referrer,
      stg.first_hit_time_gmt,
      stg.geo_city,
      stg.geo_country,
      stg.geo_dma,
      stg.geo_region,
      stg.geo_zip,
      stg.hier1,
      stg.hier2,
      stg.hier3,
      stg.hier4,
      stg.hier5,
      stg.hitid_high,
      stg.hitid_low,
      stg.hit_source,
      stg.hit_time_gmt,
      stg.homepage,
      stg.hourly_visitor,
      stg.ip,
      stg.ip2,
      stg.java_enabled,
      stg.javascript,
      stg.j_jscript,
      stg.language,
      stg.last_hit_time_gmt,
      stg.last_purchase_num,
      stg.last_purchase_time_gmt,
      stg.mobile_id,
      stg.monthly_visitor,
      stg.mvvar1,
      stg.mvvar2,
      stg.mvvar3,
      stg.namespace,
      stg.new_visit,
      stg.os,
      stg.page_event,
      stg.page_event_var1,
      stg.page_event_var2,
      stg.page_event_var3,
      stg.pagename,
      stg.page_type,
      stg.page_url,
      stg.paid_search,
      stg.partner_plugins,
      stg.persistent_cookie,
      stg.plugins,
      stg.post_browser_height,
      stg.post_browser_width,
      stg.post_campaign,
      stg.post_channel,
      stg.post_cookies,
      stg.post_currency,
      stg.post_cust_hit_time_gmt,
      stg.post_cust_visid,
      stg.post_evar1,
      stg.post_evar2,
      stg.post_evar3,
      stg.post_evar4,
      stg.post_evar5,
      stg.post_evar6,
      stg.post_evar7,
      stg.post_evar8,
      stg.post_evar9,
      stg.post_evar10,
      stg.post_evar11,
      stg.post_evar12,
      stg.post_evar13,
      stg.post_evar14,
      stg.post_evar15,
      stg.post_evar16,
      stg.post_evar17,
      stg.post_evar18,
      stg.post_evar19,
      stg.post_evar20,
      stg.post_evar21,
      stg.post_evar22,
      stg.post_evar23,
      stg.post_evar24,
      stg.post_evar25,
      stg.post_evar26,
      stg.post_evar27,
      stg.post_evar28,
      stg.post_evar29,
      stg.post_evar30,
      stg.post_evar31,
      stg.post_evar32,
      stg.post_evar33,
      stg.post_evar34,
      stg.post_evar35,
      stg.post_evar36,
      stg.post_evar37,
      stg.post_evar38,
      stg.post_evar39,
      stg.post_evar40,
      stg.post_evar41,
      stg.post_evar42,
      stg.post_evar43,
      stg.post_evar44,
      stg.post_evar45,
      stg.post_evar46,
      stg.post_evar47,
      stg.post_evar48,
      stg.post_evar49,
      stg.post_evar50,
      stg.post_evar51,
      stg.post_evar52,
      stg.post_evar53,
      stg.post_evar54,
      stg.post_evar55,
      stg.post_evar56,
      stg.post_evar57,
      stg.post_evar58,
      stg.post_evar59,
      stg.post_evar60,
      stg.post_evar61,
      stg.post_evar62,
      stg.post_evar63,
      stg.post_evar64,
      stg.post_evar65,
      stg.post_evar66,
      stg.post_evar67,
      stg.post_evar68,
      stg.post_evar69,
      stg.post_evar70,
      stg.post_evar71,
      stg.post_evar72,
      stg.post_evar73,
      stg.post_evar74,
      stg.post_evar75,
      stg.post_event_list,
      stg.post_hier1,
      stg.post_hier2,
      stg.post_hier3,
      stg.post_hier4,
      stg.post_hier5,
      stg.post_java_enabled,
      stg.post_keywords,
      stg.post_mvvar1,
      stg.post_mvvar2,
      stg.post_mvvar3,
      stg.post_page_event,
      stg.post_page_event_var1,
      stg.post_page_event_var2,
      stg.post_page_event_var3,
      stg.post_pagename,
      stg.post_pagename_no_url,
      stg.post_page_type,
      stg.post_page_url,
      stg.post_partner_plugins,
      stg.post_persistent_cookie,
      stg.post_product_list,
      stg.post_prop1,
      stg.post_prop2,
      stg.post_prop3,
      stg.post_prop4,
      stg.post_prop5,
      stg.post_prop6,
      stg.post_prop7,
      stg.post_prop8,
      stg.post_prop9,
      stg.post_prop10,
      stg.post_prop11,
      stg.post_prop12,
      stg.post_prop13,
      stg.post_prop14,
      stg.post_prop15,
      stg.post_prop16,
      stg.post_prop17,
      stg.post_prop18,
      stg.post_prop19,
      stg.post_prop20,
      stg.post_prop21,
      stg.post_prop22,
      stg.post_prop23,
      stg.post_prop24,
      stg.post_prop25,
      stg.post_prop26,
      stg.post_prop27,
      stg.post_prop28,
      stg.post_prop29,
      stg.post_prop30,
      stg.post_prop31,
      stg.post_prop32,
      stg.post_prop33,
      stg.post_prop34,
      stg.post_prop35,
      stg.post_prop36,
      stg.post_prop37,
      stg.post_prop38,
      stg.post_prop39,
      stg.post_prop40,
      stg.post_prop41,
      stg.post_prop42,
      stg.post_prop43,
      stg.post_prop44,
      stg.post_prop45,
      stg.post_prop46,
      stg.post_prop47,
      stg.post_prop48,
      stg.post_prop49,
      stg.post_prop50,
      stg.post_prop51,
      stg.post_prop52,
      stg.post_prop53,
      stg.post_prop54,
      stg.post_prop55,
      stg.post_prop56,
      stg.post_prop57,
      stg.post_prop58,
      stg.post_prop59,
      stg.post_prop60,
      stg.post_prop61,
      stg.post_prop62,
      stg.post_prop63,
      stg.post_prop64,
      stg.post_prop65,
      stg.post_prop66,
      stg.post_prop67,
      stg.post_prop68,
      stg.post_prop69,
      stg.post_prop70,
      stg.post_prop71,
      stg.post_prop72,
      stg.post_prop73,
      stg.post_prop74,
      stg.post_prop75,
      stg.post_purchaseid,
      stg.post_referrer,
      stg.post_search_engine,
      stg.post_state,
      stg.post_survey,
      stg.post_tnt,
      stg.post_transactionid,
      stg.post_t_time_info,
      stg.post_visid_high,
      stg.post_visid_low,
      stg.post_visid_type,
      stg.post_zip,
      stg.p_plugins,
      stg.prev_page,
      stg.product_list,
      stg.product_merchandising,
      stg.prop1,
      stg.prop2,
      stg.prop3,
      stg.prop4,
      stg.prop5,
      stg.prop6,
      stg.prop7,
      stg.prop8,
      stg.prop9,
      stg.prop10,
      stg.prop11,
      stg.prop12,
      stg.prop13,
      stg.prop14,
      stg.prop15,
      stg.prop16,
      stg.prop17,
      stg.prop18,
      stg.prop19,
      stg.prop20,
      stg.prop21,
      stg.prop22,
      stg.prop23,
      stg.prop24,
      stg.prop25,
      stg.prop26,
      stg.prop27,
      stg.prop28,
      stg.prop29,
      stg.prop30,
      stg.prop31,
      stg.prop32,
      stg.prop33,
      stg.prop34,
      stg.prop35,
      stg.prop36,
      stg.prop37,
      stg.prop38,
      stg.prop39,
      stg.prop40,
      stg.prop41,
      stg.prop42,
      stg.prop43,
      stg.prop44,
      stg.prop45,
      stg.prop46,
      stg.prop47,
      stg.prop48,
      stg.prop49,
      stg.prop50,
      stg.prop51,
      stg.prop52,
      stg.prop53,
      stg.prop54,
      stg.prop55,
      stg.prop56,
      stg.prop57,
      stg.prop58,
      stg.prop59,
      stg.prop60,
      stg.prop61,
      stg.prop62,
      stg.prop63,
      stg.prop64,
      stg.prop65,
      stg.prop66,
      stg.prop67,
      stg.prop68,
      stg.prop69,
      stg.prop70,
      stg.prop71,
      stg.prop72,
      stg.prop73,
      stg.prop74,
      stg.prop75,
      stg.purchaseid,
      stg.quarterly_visitor,
      stg.ref_domain,
      stg.referrer,
      stg.ref_type,
      stg.resolution,
      stg.sampled_hit,
      stg.search_engine,
      stg.search_page_num,
      stg.secondary_hit,
      stg.service,
      stg.sourceid,
      stg.s_resolution,
      stg.state,
      stg.stats_server,
      stg.tnt,
      stg.tnt_post_vista,
      stg.transactionid,
      stg.truncated_hit,
      stg.t_time_info,
      stg.ua_color,
      stg.ua_os,
      stg.ua_pixels,
      stg.user_agent,
      stg.user_hash,
      stg.userid,
      stg.username,
      stg.user_server,
      stg.va_closer_detail,
      stg.va_closer_id,
      stg.va_finder_detail,
      stg.va_finder_id,
      stg.va_instance_event,
      stg.va_new_engagement,
      stg.visid_high,
      stg.visid_low,
      stg.visid_new,
      stg.visid_timestamp,
      stg.visid_type,
      stg.visit_keywords,
      stg.visit_num,
      stg.visit_page_num,
      stg.visit_referrer,
      stg.visit_search_engine,
      stg.visit_start_pagename,
      stg.visit_start_page_url,
      stg.visit_start_time_gmt,
      stg.weekly_visitor,
      stg.yearly_visitor,
      stg.zip,
      stg.mobileaction,
      stg.mobileappid,
      stg.mobilecampaigncontent,
      stg.mobilecampaignmedium,
      stg.mobilecampaignname,
      stg.mobilecampaignsource,
      stg.mobilecampaignterm,
      stg.mobiledayofweek,
      stg.mobiledayssincefirstuse,
      stg.mobiledayssincelastuse,
      stg.mobiledevice,
      stg.mobilehourofday,
      stg.mobileinstalldate,
      stg.mobilelaunchnumber,
      stg.mobileltv,
      stg.mobileosversion,
      stg.mobileresolution,
      stg.pointofinterest,
      stg.pointofinterestdistance,
      stg.post_mobileaction,
      stg.post_mobileappid,
      stg.post_mobilecampaigncontent,
      stg.post_mobilecampaignmedium,
      stg.post_mobilecampaignname,
      stg.post_mobilecampaignsource,
      stg.post_mobilecampaignterm,
      stg.post_mobiledayofweek,
      stg.post_mobiledayssincefirstuse,
      stg.post_mobiledayssincelastuse,
      stg.post_mobiledevice,
      stg.post_mobilehourofday,
      stg.post_mobileinstalldate,
      stg.post_mobilelaunchnumber,
      stg.post_mobileltv,
      stg.post_mobileosversion,
      stg.post_mobileresolution,
      stg.post_pointofinterest,
      stg.post_pointofinterestdistance,
      stg.socialassettrackingcode,
      stg.socialauthor,
      stg.socialaveragesentiment,
      stg.socialcontentprovider,
      stg.sociallanguage,
      stg.sociallatlong,
      stg.sociallink,
      stg.socialproperty,
      stg.socialterm,
      stg.socialtermslist,
      stg.post_socialassettrackingcode,
      stg.post_socialauthor,
      stg.post_socialaveragesentiment,
      stg.post_socialcontentprovider,
      stg.post_sociallanguage,
      stg.post_sociallatlong,
      stg.post_sociallink,
      stg.post_socialproperty,
      stg.post_socialterm,
      stg.post_socialtermslist,
      stg.video,
      stg.videoad,
      stg.videoadinpod,
      stg.videoadplayername,
      stg.videoadpod,
      stg.videochannel,
      stg.videocontenttype,
      stg.videopath,
      stg.videoplayername,
      stg.videosegment,
      stg.post_video,
      stg.post_videoad,
      stg.post_videoadinpod,
      stg.post_videoadplayername,
      stg.post_videoadpod,
      stg.post_videochannel,
      stg.post_videocontenttype,
      stg.post_videopath,
      stg.post_videoplayername,
      stg.post_videosegment,
      CONCAT(year(date_time),'-',
            CASE WHEN month(date_time) < 10 THEN concat('0',month(date_time)) ELSE trim(month(date_time)) END,'-',
            CASE WHEN day(date_time) < 10 THEN concat('0',day(date_time)) ELSE trim(day(date_time)) END) as m_date
DISTRIBUTE BY m_date;

DROP TABLE ${HIVE_STAGING_TABLE};
EOF

echo "$program: Running load to prod hive HQL from $hql_script_file" 1>&2
cmd="hive $HIVE_OPTS -f $hql_script_file"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd :" 1>&2
  cat $hql_script_file
else
  echo "$program: running hive $cmd"
  $cmd 2>&1 | tee $hive_out_file
  status=$?
  sed -e 's/^/    /' $hive_out_file 1>&2
  if test $status != 0; then
    echo "$program: FAILED load to prod hive $cmd with result $status" 1>&2
    exit $status
  fi
fi


cmd="hadoop fs -chmod -R 755 ${HIVE_PROD_TABLE_DIR}"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  $cmd
  status=$?
  if test $status != 0; then
    echo "$program: FAILED $cmd with result $status" 1>&2
    exit $status
  fi
fi


# Build HQL to count production table for date (validation)
cat > $hql_script_file <<EOF
USE ${HIVE_DATABASE};

SELECT dt, COUNT(1) from ${HIVE_PROD_TABLE}
WHERE dt >="${start_date}" AND dt <= "${end_date}"
GROUP BY dt
ORDER BY dt ASC
;
EOF

echo "$program: Running validate hive HQL from $hql_script_file" 1>&2
cmd="hive $HIVE_OPTS -f $hql_script_file"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd :" 1>&2
  cat $hql_script_file
else
  echo "$program: running $cmd"
  $cmd > $hive_out_file 2> $hive_err_file
  status=$?
  if test $status != 0; then
    echo "Hive stdout:" 1>&2
    sed -e 's/^/    /' $hive_out_file 1>&2
    echo "Hive stderr:" 1>&2
    sed -e 's/^/    /' $hive_err_file 1>&2
    echo "$program: FAILED validation hive $cmd with result $status" 1>&2
    exit $status
  fi
  echo "$program: Validation hive returned:" 1>&2
  sed -e 's/^/    /' $hive_out_file 1>&2
fi


# Delete working dir
cmd="hadoop fs -rmr $HDFS_WORKING_DIR"
if test $dryrun = 1; then
  echo "$program: DRYRUN: would run $cmd" 1>&2
else
  echo "$program: running $cmd"
  $cmd
  status=$?
  if test $status != 0; then
    echo "$program: FAILED $cmd with result $status" 1>&2
    exit $status
  fi
fi
