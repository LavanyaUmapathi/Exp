'''
Created on Mar 1, 2016

@author: natasha.gajic

Input arguments:
    ODS_SQL_SERVER_IP
    ODS_DB_NAME
    SQL_SERVER_USERNAME
    SQL_SERVER_PASSWORD
    CONFIGURATION_FILE
    OUTPUT_DIR
    RECREATE_ODS

Note: Configuration file is csv file. It contains the list of all tables from specific ODS
that should be archived into Hadoop. The csv file columns are:
sql_table_owner
sql_table_name
row_count   *** This first 3 fields are output from anayze_ods.py
partitioned  *** This is a flag Y/N. As the result of the table analysis decide whether to
             *** partition table or not. Partitions are by date. Tables where high number of
             *** records (approximately: more than 500000) is added daily should be considered
             *** for partitioning
incremental  *** This is a flag Y/N. If the table is partitioned then it is incremental. If the
             *** table is not partitioned then it can be a full load or incremental with append.
             *** Small static tables are good candidates for a full load. Tables where smaller
             *** number of records is added daily (approximately: less than 500000) should be
             *** considered for incremental not partitioned tables
extraction_field  *** For incremental load specify a field for a where condition. This field
                  *** should contain date
comment  *** hive table comment. If omitted the default comment will be added

Based on the information provided in the config file this script will :
1. Create Hive databases using WebHcat
2. Create all Hive tables
3. Set of shell scripts to archive all tables
4. Start date files for all tables in incremental load
5. Corresponding Appache Airflow DAG file

'''
import os
import sys

import constants as cons
import jinja2
import sqlserver_api as sql_api
import webhcat_api as hcat
import re

def _validate_column_name(cname):
        if cname in cons.INVALID_COLUMN_NAMES:
                return cname+'1000'
        return cname


def assign_table_to_script(file_dict, sorted_config):
    table_to_script = {}
    i = 0
    for one_config in sorted_config:
        if i == len(file_dict):
            i = 0
        table_to_script[one_config[1]] = file_dict[i][1]
        i = i + 1
    return table_to_script

def get_airflow_path(script_name):
    return os.path.join(cons.AIRFLOW_SCRIPTS_DIR, script_name)

def get_command(exe_name, args_common, args_extra):
    return "{0} {1} {2}\n".format(exe_name, ' '.join(args_common), ' '.join(args_extra))

def main():
    sql_ip = sys.argv[1]
    sql_db_name = sys.argv[2]
    sql_uname = sys.argv[3]
    sql_pwd = sys.argv[4]
    config_file_name = sys.argv[5]
    scripts_dir = sys.argv[6]
    re_create = sys.argv[7]

    hcat_table_list = []

    if re_create == 'False':
        hcat_table_list = hcat.get_tables(sql_db_name)


    if not os.path.isdir(scripts_dir):
        os.makedirs(scripts_dir)
    else:
        for fname in os.listdir(scripts_dir):
            if fname.endswith('.sh') or fname.endswith('.py'):
                os.remove(os.path.join(scripts_dir, fname))


    dates_dir = os.path.join(scripts_dir, 'dates')
    if not os.path.isdir(dates_dir):
        os.makedirs(dates_dir)
    else:
        for fname in os.listdir(dates_dir):
            os.remove(os.path.join(dates_dir, fname))

    file_dict = []

    for i in range(0, cons.SPLIT_NUMBER):
        file_name = os.path.join(scripts_dir, "{0}_{1}.sh".format(cons.SHELL_SCRIPT_BASE_NAME, i))
        file_dict.append((file_name, open(file_name, 'w')))

    one_time_load_script = open(os.path.join(scripts_dir, cons.ONE_TIME_LOAD_SHELL_SCRIPT), 'w')

    conn = sql_api.get_connection(sql_ip, sql_uname, sql_pwd, sql_db_name)


    hcat.create_database(sql_db_name, sql_db_name + " archive")
    with open(config_file_name) as config_file:
        config_data = [line.split(',') for line in config_file]
        if not config_data[0][2].isdigit():
            config_data.pop(0)
        sorted_config = sorted(config_data, key=lambda x: int(x[2]), reverse=True)

        table_to_script_dict = assign_table_to_script(file_dict, sorted_config)

        for config_line in sorted_config:
            (table_owner, table_name, row_count, _index_count, partition_flag, incremental_flag,
             comment, extraction_field, istimestamp, isdate, orc_table, one_time_load, _index_info,
             _first_load_date,
             remove_dups_flag, remove_dup_pk,remove_dup_date_field,remove_dup_ts_field,remove_dup_ms_field) = config_line
            one_time_load = one_time_load.strip()
            external_flag = True
            isdate = isdate.strip()
            if partition_flag == 'N' and incremental_flag == 'N':
                external_flag = False
            update_time_field_name=''
            isupdatedtimestamp=False
            isupdatedmillisec=False
            if remove_dups_flag == 'Y':
                partition_flag = 'N'
                incremental_flag = 'Y'
                if len(remove_dup_date_field.strip())>0:
                    update_time_field_name=remove_dup_date_field
                else:
                    if len(remove_dup_ts_field)>0:
                        update_time_field_name=remove_dup_ts_field
                        isupdatedtimestamp=True
                    else:
                        update_time_field_name=remove_dup_ms_field
                        isupdatedmillisec=True
            
            if table_name.lower() not in hcat_table_list:
                sql_table_col = sql_api.get_columns(conn, table_name, table_owner)
                partition_col = []
                table_col = []
                for one_col in sql_table_col:
                    table_col.append((_validate_column_name(re.sub('[^0-9a-z]+', '_',one_col[0].lower())), cons.HIVE_STRING_DATATYPE))


                if partition_flag == 'Y':
                    partition_col.append((cons.DATE_PARTITION_COL_NAME,
                                          cons.HIVE_STRING_DATATYPE))
                if len(comment.strip()) == 0:
                    comment = "Archive of :" + table_name.lower()
                hcat.create_table(sql_db_name, table_name.lower(), comment, table_col,
                                  partition_col, external_flag)
                if orc_table == 'Y':
                    hcat.create_orc_table(sql_db_name, table_name.lower() + '_orc', comment,
                                          table_col, partition_col, external_flag)
                if remove_dups_flag == 'Y':
                    hcat.create_orc_table(sql_db_name, table_name.lower() + cons.LATEST_TABLE_NAME, comment,
                                          table_col, partition_col, external_flag)
                if partition_flag == 'Y' and incremental_flag == 'N':
                    hcat.create_orc_table(sql_db_name, table_name.lower() + cons.CURRENT_TABLE_NAME, comment,
                                          table_col, [], external_flag)

                
                print "Table created:" + table_name.lower() + "\n"
            orc_flag = 'true' if orc_table == 'Y' else 'false'
            remove_dup_cmd=''
            if not external_flag:
                if row_count == '0':
                    table_to_script_dict[table_name].write(cons.COMMENT_LINE)

                exe = get_airflow_path(cons.FULL_LOAD_SHELL_SCRIPT)
                common_args = [sql_db_name.lower(), table_name.lower(), table_owner, orc_flag]
                extra_args = []
            else:
                common_args = [sql_db_name.lower(), table_name.lower(), extraction_field.lower(),
                               table_owner, orc_flag]
                if remove_dups_flag == 'Y':
                    exe = get_airflow_path(cons.NON_PARTITION_INCREMENTAL_LOAD_SCRIPT)
                    if isdate == 'Y':
                        extra_args = ['true', 'true'] if istimestamp == 'Y' else ['true', 'false']
                    else:
                        extra_args = ['false', 'false']

                else:

                    if partition_flag == 'Y':
                        if incremental_flag == 'Y':
                            exe = get_airflow_path(cons.PARTITION_INCREMENTAL_LOAD_SCRIPT)
                            extra_args = ['true', 'true'] if istimestamp == 'Y' else ['true', 'false']
                        else:
                            exe = get_airflow_path(cons.FULL_LOAD_PARTITIONED_TABLE_SCRIPT)
                            extra_args = ['true', 'true'] if istimestamp == 'Y' else ['true', 'false']

                    else:
                        exe = get_airflow_path(cons.NON_PARTITION_INCREMENTAL_LOAD_SCRIPT)
                        if isdate == 'Y':
                            extra_args = ['true', 'true'] if istimestamp == 'Y' else ['true', 'false']
                        else:
                            extra_args = ['false', 'false']

            command = get_command(exe, common_args, extra_args)
            if remove_dups_flag == 'Y':
                remove_dup_cmd="{0} {1} {2} {3} {4} {5} {6}\n".format(\
                                get_airflow_path(cons.REMOVE_DUPLICATES_SCRIPT),\
                                sql_db_name.lower(), table_name.lower(),\
                                remove_dup_pk.lower(),update_time_field_name.lower(),\
                                str(isupdatedtimestamp).lower(),str(isupdatedmillisec).lower())
                command=command+remove_dup_cmd

            if one_time_load == 'Y':
                one_time_load_script.write(command)
            else:
                table_to_script_dict[table_name].write(command)



            #For incremental load get min date and create date file
            if incremental_flag == 'Y':
                with open(os.path.join(dates_dir, table_name.lower()), 'w') as date_file:
                    if isdate == 'Y':
                        if istimestamp == 'Y':
                            date_file.write(sql_api.get_min_date(conn, table_name, extraction_field,
                                                                 table_owner, True))
                        else:
                            date_file.write(sql_api.get_min_date(conn, table_name, extraction_field,
                                                                 table_owner, False))
                    else:
                        date_file.write('0')


    for one_file in file_dict:
        one_file[1].close()
    one_time_load_script.close()

    #Create airflow DAG file
    dag_template = jinja2.Template(cons.DAG_TEMPLATE)
    with open(os.path.join(scripts_dir, sql_db_name.lower() + '.py'), 'w') as dag_file:
        dag_file.write(dag_template.render(ods_name=sql_db_name.lower(),
                                           split_cnt=cons.SPLIT_NUMBER,
                                           script_name=cons.SHELL_SCRIPT_BASE_NAME))


if __name__ == '__main__':  # pragma: nocover
    main()
    sys.exit(0)
