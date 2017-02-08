#!/usr/bin/env python

import argparse
import json
import logging
import sys
import tempfile
from datetime import datetime

import dateutil.parser
import requests

import hdfs


LOG = logging.getLogger(__name__)
BASE_URL = 'https://synthetics.intensive.int/api/v1.0/bi'
BASE_HDFS_PATH = '/user/maas'
POLLS_JSON_DEFAULT = '{ "checks": [], "count": 0 }'

#calc start/end datestamps
def get_dates(for_date):
    arg_date = dateutil.parser.parse(for_date)
    beg_date = arg_date.strftime('%Y%m%d000000')
    end_date = arg_date.strftime('%Y%m%d235959')
    return (beg_date, end_date)

def get_api_key():
    return 'fcc62ba4-1976-4f3a-b554-0c0b0721b5f3'

def get_json(url, default=None):
    LOG.debug("Getting data from " + url)
    headers = {'X-Api-Key': get_api_key()}
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200 and len(response.text) > 3:
        return json.loads(response.text)

    if response.status_code == 404 and default:
        return json.loads(default)

    LOG.error("API call failed\n     URL: %s\n  Status: %s\n    Text: %s", \
        url, response.status_code, response.text)
    raise Exception('API call failed for url: ' + url)

def get_monitors_url():
    return '{0}/monitors'.format(BASE_URL)

def get_polls_url(dates, monitor_id):
    return '{0}/{1}?startDateTime={2}&endDateTime={3}'.format(
        BASE_URL, monitor_id, dates[0], dates[1])

def get_monitors():
    resp_json = get_json(get_monitors_url())
    monitors = resp_json["monitors"]
    count = resp_json["count"]
    if len(monitors) != count:
        raise Exception('Expected ' + count + ' monitors, received ' + len(monitors))

    return monitors

def get_polls(dates, monitor_id):
    resp_json = get_json(get_polls_url(dates, monitor_id), default=POLLS_JSON_DEFAULT)
    polls = resp_json["checks"]
    count = resp_json["count"]
    if len(polls) != count:
        raise Exception('Expected ' + count + ' checks, received ' + len(polls))

    for poll in polls:
        poll_time = datetime.fromtimestamp(poll['time']).strftime('%Y%m%d%H%M%S')
        if poll_time < dates[0] or poll_time > dates[1]:
            raise Exception('Poll time {0} ({1}) out of range {2} - {3}').format(
                poll['time'], poll_time, dates[0], dates[1]
            )

    return resp_json

def upload_to_hdfs(local, remote_dir, remote_file):
    LOG.info("Copying local %s to HDFS %s/%s", local, remote_dir, remote_file)

    if not hdfs.file_exists(remote_dir):
        hdfs.make_dir(remote_dir)

    remote_name = "{0}/{1}".format(remote_dir, remote_file)
    if hdfs.file_exists(remote_name):
        hdfs.rm_file(remote_name)

    hdfs.move_from_local(local, remote_name)

def fetch_data(for_date):
    LOG.info("Starting")
    dates = get_dates(for_date)

    cnt_monitors = 0
    cnt_polls = 0

    try:
        monitors_file = tempfile.NamedTemporaryFile(delete=False)
        polls_file = tempfile.NamedTemporaryFile(delete=False)

        for monitor in get_monitors():
            json.dump(monitor, monitors_file)
            monitors_file.write("\n")

            poll = get_polls(dates, monitor["id"])
            json.dump(poll, polls_file)
            polls_file.write("\n")

            cnt_monitors += 1
            cnt_polls += poll["count"]
    finally:
        monitors_file.close()
        polls_file.close()

    file_date = for_date.replace('-', '')
    file_name = "{0}-000.json".format(file_date)

    monitors_dir = '{0}/newrelic-monitors/{1}'.format(BASE_HDFS_PATH, for_date)
    upload_to_hdfs(monitors_file.name, monitors_dir, file_name)

    polls_dir = '{0}/newrelic-polls/{1}'.format(BASE_HDFS_PATH, for_date)
    upload_to_hdfs(polls_file.name, polls_dir, file_name)

    LOG.info("Completed successfully: %d monitors and %d polls ingested", cnt_monitors, cnt_polls)

def _parse_args():
    parser = argparse.ArgumentParser(description='New Relic Metrics Ingester')
    parser.add_argument('date', help='Date to retrieve data for (YYYY-MM-DD)')
    parser.add_argument('--debug', help='Emit debug level logging', action='store_true')
    return parser.parse_args()

def _init_logging(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level)
    logging.getLogger('requests').setLevel(logging.ERROR)

def main():
    args = _parse_args()

    _init_logging(args.debug)
    fetch_data(args.date)

if __name__ == '__main__':  # pragma: nocover
    main()
    sys.exit(0)
