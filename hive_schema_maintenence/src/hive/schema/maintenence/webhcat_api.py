'''
Created on Feb 24, 2016

@author: natasha.gajic
'''
try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client

import json
import logging
import requests

import src.hive.schema.maintenence.constants as cons

http_client.HTTPConnection.debuglevel = 1


def _get(url, accept=(200), params=None):
    new_url = cons.BASE_URL + url
    new_params = {'user.name': cons.WEBHCAT_USERNAME}
    new_params.update(params)
    resp = requests.get(new_url, params=new_params)
    if resp.status_code not in accept:
        raise ValueError('GET {url} {status}'.format(url=resp.url, status=resp.status_code))
    return resp

def _get_json(url, params=None):
    return json.loads(_get(url, params).text)

def _put(url, data):
    new_url = cons.BASE_URL + url
    new_params = {'user.name': cons.WEBHCAT_USERNAME}
    resp = requests.put(new_url, data=data, params=new_params,
                        headers={'Content-Type':'application/json'})
    if resp.status_code != 200:
        raise ValueError('PUT {url} {status}'.format(url=resp.url, status=resp.status_code))
    return resp

def _get_cols(columns):
    cols = []
    for (column_name, column_type) in columns:
        cols.append({"name": column_name, "type": column_type})
    return cols

def _set_log_level(level):
    logging.basicConfig()
    logging.getLogger().setLevel(level)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(level)
    requests_log.propagate = True

def get_databases():
    db_list = []

    jobj = _get_json('ddl/database')
    for one_db in jobj['databases']:
        db_list.append(one_db)

    return db_list

def get_tables(db_name):
    table_list = []

    jobj = _get_json('ddl/database/' + db_name + '/table')
    for one_table in jobj['tables']:
        table_list.append(one_table)

    return table_list

def get_table_columns(db_name, table_name):
    table_columns = []
    jobj = _get_json('ddl/database/' + db_name + '/table/' + table_name,
                     params={'format': 'extended'})
    for one_column in jobj['columns']:
        table_columns.append((one_column['name'].encode('ascii'),
                              one_column['type'].encode('ascii')))
    return table_columns

def database_exists(db_name):
    return _get('ddl/database/' + db_name, accept=(200, 404)).status_code == 200

def table_exists(db_name, table_name):
    url = 'ddl/database/' + db_name + '/table/' + table_name
    return _get(url, accept=(200, 404)).status_code == 200

def create_database(db_name, comment):
    if not database_exists(db_name):
        db_json = {"comment": comment}
        _put('ddl/database/' + db_name, db_json)
    return True

def create_table(db_name, table_name, comment, columns, partition_by, external_flag,
                 format_dict=None):

    if format_dict is None:
        format_dict = cons.DEFAULT_FORMAT_DICT

    if table_exists(db_name, table_name):
        drop_table(db_name, table_name)

    table_json = {
        "external": str(external_flag).lower(),
        "comment": comment,
        "columns": _get_cols(columns),
        "partitionedBy": _get_cols(partition_by),
        "format": format_dict
    }

    _set_log_level(logging.ERROR)

    _put('ddl/database/' + db_name + '/table/' + table_name, table_json)

    return True

def create_orc_table(db_name, table_name, comment, columns, partition_by, external_flag):
    return create_table(db_name, table_name, comment, columns, partition_by, external_flag,
                        format_dict=cons.ORC_FORMAT_DICT)

def drop_table(db_name, table_name):
    resp = requests.delete('ddl/database/' + db_name + '/table/' + table_name)
    if resp.status_code != 200:
        raise ValueError('DELETE {url} {status}'.format(url=resp.url, status=resp.status_code))

    return True

def run_query(query, hdfs_loc):
    _set_log_level(logging.INFO)
    params_values = {'execute' : query, 'statusdir' : hdfs_loc, 'user.name': cons.WEBHCAT_USERNAME}

    resp = requests.post(cons.QUERY_URL, params=params_values)
    if resp.status_code != 200:
        raise ValueError('GET {url} {status}'.format(url=resp.url, status=resp.status_code))

    return resp
