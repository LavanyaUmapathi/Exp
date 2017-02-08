Billing Events schema notes
===========================

* Billing_Events_Fields_list.rtf
    Original Postgres schema with all fields.
* EBI_HiveTable.rtf
    Hive schema file - much fewer fields
* billing_events.mysql
    MySQL schema for billing events with all fields (tested)
* billing_events.pgsql
    PostGres schema for billing events with all fields (not tested but based on original)
* csv-to-load-data.sh
    Script to extract some data from csv.gz source and fixup NULLs
    for MySQL


Not included here but for information:

`billing_events_20140430.csv.gz`

* 1005380969 gzipped bytes (1.01GB)
* 5748186 records (5.7M)
* 11304202157 raw bytes (10.53GB)


Dave Beckett

Tue Jan 13 09:27:55 PST 2015
