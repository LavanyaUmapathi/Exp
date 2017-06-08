# Creation of ODS Archiving Process

## Purpose

The purpose of the document is to outline steps for creation of archival process of SQL Server ODS to Hadoop V2.

## Introduction

### Use Cases

There are four use case scenarios for archival of SQL Server ODS table to Hadoop/Hive  V2. The use cases are:

1. Truncate and Load

    This use case is applicable for small configuration tables where we do not preserve history. The shell script that performs this archiving is: [ods\_full\_load.sh](https://github.com/racker/volga/blob/master/ods-archiving/scripts/ods_archiving/ods_full_load.sh)

2. Full snapshot load into partitioned table

    This use case is applicable for ODS tables that contain less than 10 million records. Those are tables where source system modifies records and also deletes historical information to maintain the table size.  In this scenario full table is archived to a daily partition in Hive table. This way a history is preserved in Hadoop. The daily partition is for the date when archiving is performed.  This is the most common use case. The shell script that performs this archiving use case is: [ods\_full\_partitioned\_table\_load.sh](https://github.com/racker/volga/blob/master/ods-archiving/scripts/ods_archiving/ods_full_partitioned_table_load.sh)

3. Incremental load into partition table

    This use case is applicable for large ODS tables where there are no updates and significant number of records is added daily.  Records for each day will be archived in the corresponding daily partition on Hadoop/Hive side. It is important that all queries on the Hive side filter records by daily partition. Otherwise, the queries will be very slow. The shell script that performs this archiving use case is: [ods\_incremental\_partitioned\_table\_load.sh](https://github.com/racker/volga/blob/master/ods-archiving/scripts/ods_archiving/ods_incremental_partitioned_table_load.sh)

4. Incremental load into non-partition table. 

    This use case is applicable for large ODS tables where there are updates and significant number of records is added daily. These tables are not queried by day. In this tables the new records are appended daily. The shell script that performs this archiving use case is: [ods\_incremental\_non\_partitoned\_table\_load.sh](https://github.com/racker/volga/blob/master/ods-archiving/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh)

### Overview

Archiving ODS to Hadoop/Hive is 4-step process:

1. Gather information about ODS tables
2. Make decision on which use case to apply to each table
3. Generate ODS archiving scripts
4. Push changes to git

Steps 1 & 3 have been automated. Step 2 is a manual process. 

The code for steps 1 & 3 is in:
[hive\_schema\_maintenance](https://github.com/racker/volga/tree/master/hive_schema_maintenance/)

## Instructions

### Prerequisites

1.	Install Anaconda Python distribution.
2.	Check out racker/volga project from git (TODO: add link to instructions)
3.	Set up the following environment variables
    - PYTHONHOME - to point to anaconda installation
    - PATH - to include PYTHONHOME
    - PYTHONPATH to include hive_schema_maintenence directory
    - Open tunnel to Hive port admin1.prod.iad.caspian.rax.io:10000 (TODO: add link to instructions)
4. Install pypyodbc and jinja2 packages from pip:

    `pip install pypyodbc jinja2`

If you're on a Mac, you'll also need the following:

1. Install homebrew (TODO: add link to instructions)
2. Install FreeTDS with Unix ODBC optional features:
    `brew install freetds --with-unixodbc`
3. Install Unix ODBC:
    `brew install unixodbc`
4. Add a SQL Server entry to your odbcinst.ini

### Step 1 - Gather Information About ODS Tables

Python script analyze_ods.py will provide report about the ODS tables. The report includes:

1.	Table name
2.	Record count
3.	Index count
4.	Index information  for each index provides list of columns
5.	Minimum value in ODS dw_timestamp field. If the value in this field is a today's date the SQL ODS Load is full load
6.  remove_duplicates(default N), remove_dup_pk_field,remove_dup_date_field,remove_dup_ts_field, remove_dup_ts_ms_field


Execute analyze_ods.py script:

    python analyze_ods.py EBI-ODS-CORE-IP-ADDRESS ODS_NAME USERNAME PASSWORD PATH_TO_REPORT_FILE/ODS_NAME.csv
    
### Step 2 - Analyze ODS Report and Specify Archiving Use Case

Use Excel to open PATH\_TO\_REPORT\_FILE/ODS\_NAME.csv. By looking into table information populate following fields:

1.	Partitioned: values Y or N
    
    If the value is Y the table will be partitioned. This is applicable for use cases 2 & 3 . Otherwise the table will not be partitioned - use cases 1 & 4.
    
2.	Incremental: values Y or N

    If the value is Y the table load will be incremental  - use cases 3 &4. Otherwise the load will be full - use cases 1 & 2
    
3.	Extraction field. In case of incremental load the incremental field has to be specified. Usually that is a date field. By default dw_timestamp is specified. You can change it to another field that represents insert date or similar attribute. In case that the date field is not the one to be used specify the name of the field that should be used. Usually that would be a record ID field. NOTE: In case of the full load please do not remove default value in this field (dw_timestamp)

4.	IsTimestamp: values Y or N.  Indication that incremental load date field is in timestamp format in SQL Server ODS table. If the field is timestamp then Y otherwise N

5.	IsDate: values Y or N. If Y then incremental load field is in date format. 

6.	OrcTable: values Y or N. Indication whether to create an ORC table for the corresponding ODS table. If Y the ORC table will be created in addition to gz table

7.	OneTimeLoad: values Y or N. Usually N. To be used to perform one tie archiving. To be used on tables that do not change in ODS.

8.  remove_duplicates. The default value is N. The value Y is applicable for very large tables that have daily updates (ex: brm: item_t, event_t)
	The tables that have a value set to Y will be archived by incremental load into non-partitioned table.
	Archiving extraction is done by modification date. As the result there on the Hadoop side we will have
	multiple versions of updated records, while on ODS side there will be a single record. There is a
	value in maintaining the versions of the record. At the same time that can be confusing, so the flag set to Y
	instruct the framework to create a table called *_current where the last version of 2 years old
	data is populated. The framework will also generate a call to get_last_records.sh to perform de-duplication
9.  remove_dup_pk_field - duplicates are determined based on the table PK fields (comma separated list). Type in the pk fields
10. remove_dup_date_field - if modification_date is datetime format enter the field name here
11. remove_dup_ts_field - if modification_date is timestamp enter the field name here 
12. remove_dup_ts_ms_field - if modification_date is timestamp in ms enter the field name here 

Assumptions:

- Archival is running once per day

Additional notes:

File [constants.py](https://github.com/racker/volga/blob/master/hive_schema_maintenence/constants.py) contains all constants. A couple of constants might require changing:

1. "default\_args" has start time of the archival as 13:00:00 UTC. Please verify that this time will work for the specific ODS. If not set the time accordingly 

2. "SPLIT_NUMBER" has a number of parallel processes that will execute archiving. Default value is 5. For large ODSs that have hundreds of tables this number can be increased.

After all fields for all tables are populated the archiving process can be created.

### Step 3 - Create ODS Archiving Scripts

After the archiving use case for each table has been selected the archiving process can be created. The process has been automated and it will:

1. Create Hive DB and all tables using WebHCat
2. Create shell scripts that call one of use case script for each ODS table. Number of shell scripts depends on value in `SPLIT_NUMBER` constant in `constants.py`
3. Create python DAG file
4. For each  incremental load tables create marker file containing starting date or ID 

Example:

    python create_hive_ods.py  EBI-ODS-CORE-IP-ADDRESS ODS_NAME USERNAME PASSWORD PATH_TO_REPORT_FILE/ODS_NAME.csv PATH_TO_DAG_AND_SHELL_SCRIPTS True
        
PATH\_TO\_DAG\_AND\_SHELL\_SCRIPTS - recommendation is to create a folder where create\_hive\_ods.py will place:

- All shell scripts
- DAG file
- Marker Files

### Step 4 - Push Changes to github

After the review all generated scripts should be pushed to git. Git folder for ODS archiving scripts is:
[ods-archiving](https://github.com/racker/volga/tree/master/ods-archiving)

In your git repo:

1. Create script/ODSNAME folder  __NOTE: Actual ODSNAME value should be all lower case!!__
2. Create dates/ODSNAME folder   __NOTE: Actual ODSNAME value should be all lower case!!__
3. Copy all ods\_load\_batch\_\*.sh scripts to  script/ODSNAME folder.
4. Copy DAG ods\_name.py to dags folder under ods-archiving folder
5. Copy all markers to  dates/ODSNAME folder
6. Commit & push changes to git

From there Jenkins job will pick up and push the changes to api1 and execute the first archiving process. You can monitor the process in airflow UI.

### Production Environment

Jenkins will push the scripts to `api1.prod.iad.caspian.rax.io` as follows:

1. ods\_load\_batch*.sh scripts to /home/airflow/airflow-jobs/scripts/ODS_NAME
2. ods\_name.py - a dag file to / home/airflow/airflow-jobs/dags
3. marker files are copied to /var/run/ods_archive/ODS_NAME folder.
4. Additionally processControlFlag file is created in  /var/run/ods\_archive/ODS\_NAME folder. The purpose of this file is to gracefully stop archiving process. The content of this file is 0 or 1. When content of the file is 0 the archiving runs. When the value is set to 1 the process will stop on the next iteration or it will not start if the process is not running at the time of the change. This is the most useful during the first archiving process for incremental load. For very large tables (ex: cloud usage events) incremental load runs in loop: one iteration for one day. In case of large tables the initial process can take several days. The processControlFlag can be used to gracefully stop the process and set markers so that archiving can re-start.  
