#!/usr/bin/env python

"""Copy metrics files from cloud files into HDFS

This does the work so that the Metrics Loader job has the JSON
metrics files it needs in HDFS.  It replaces the Play acumen-data-api
application and is intended to be invoked by azkaban.

USAGE: cloud-file-metrics-copy.py SOURCE-CONFIG

Config files are JSON with fields like this:

{
    "authurl" : "https://auth.api.rackspacecloud.com/v1.0",
    "user" : "my-user-name",
    "key" : "my-api-key",
    "container" : "my-container"
}

The config files may also contain keys such as 'auth_version'
(e.g. 2.0, default is 1.0) and 'os_options' which takes a dictionary
of additional openstack auth parameters such as 'region_name' and
'tentant_name'.  2.0 auth requires a different 'authurl' which could
be https://identity.api.rackspacecloud.com/v2.0 for Rackspace and
the 'key' is the user's password, not API key.  Blame python-swiftclient.

The files are dropped into the tree below the 'hdfs_metrics_dir'.

<hdfs_metric_dir>:
  SUBDIR3/

  /ready/SUBDIR1/
  /ready/SUBDIR2/...

SUBDIR1, SUBDIR2... are ready for processing
SUBDIR3 is created by this script and the metrics files are stored here.
When all files are ddone, SUBDIR1 is moved into /ready/SUBDIR3.

The dir names are random alphanumerics
"""

import argparse
import json
import logging
import random
import os
import string
import sys
import tempfile
from time import time
import StringIO
from gzip import GzipFile

# local
from swiftclient import Connection
from cloudfiles import get_conn, BadConfigurationException, read_config
from common import *


# logger for this program
logger = logging.getLogger("cloud-files-metrics-copy")


def copy_metrics(input_config, hdfs_metrics_dir, marker_filename,
                 min_files=None, max_files=None,
                 gzip_data=True, verbose=False, dryrun=False):
    """Copy SWIFT files from account / container from @input_config
    to the HDFS area @output_metrics_dir.

    Copy at least @min_files and at most @max_files and store state
    in file @marker_filename.  If @gzip is True, gzip the files if they
    are not already (file name ends in '.gz')

    """

    input_connection = get_conn(input_config)
    input_container = input_config['container']

    new_dir = ''.join(random.choice(string.letters + string.digits) for x in range(20))

    marker = None
    try:
        with open(marker_filename) as fp:
            data = fp.readlines()
            marker = data[0].strip()
    except Exception, e:
        pass
    logger.debug("Marker from %s is '%s'" % (marker_filename, (marker or "None")))

    (headers, input_objs) = input_connection.get_container(input_container,
                                                           marker=marker)

    logger.debug("Container Response Headers: %s" % (str(headers), ))
    input_names = [o['name'] for o in input_objs]
    input_files_count = len(input_names)
    logger.info("Found %d files to copy", input_files_count)
    if verbose and input_files_count > 0:
        logger.info("File names range: %s ... %s" % (input_names[0], input_names[-1]))

    if min_files is not None and input_files_count < min_files:
        if input_files_count > 0:
            logger.info("Found %d files to copy but min is %d - ending" % (input_files_count, min_files))
        return

    # Copy the files
    output_hdfs_working_path = hdfs_metrics_dir + '/' + new_dir
    output_hdfs_final_path = hdfs_metrics_dir + '/ready/' + new_dir

    hadoop_make_dir(output_hdfs_working_path)

    logger.info("Copying from container %s to HDFS path %s" % (input_container, output_hdfs_working_path))


    files_count = 0

    for input_obj in input_objs:
        logger.debug("Input object: %s" % (str(input_obj), ))
        (input_name, input_length) = (input_obj['name'], input_obj['bytes'])

        logger.info("Copying '%s' (%d bytes)" % (input_name, input_length))
        input_start_time = time()
        headers, body = input_connection.get_object(input_container, input_name)
        input_end_time = time()

        input_length = len(body)
        if verbose:
            logger.info("Read '%s' (%d bytes)" % (input_name, input_length))

        logger.debug("File Response Headers: %s" % (str(headers), ))

        output_name = input_name
        output_length = input_length

        # To gzip or not gzip, that is the question
        if not input_name.endswith('.gz') and gzip_data:
            output = StringIO.StringIO()
            output_name += ".gz"
            with GzipFile(filename=output_name, mode='w', fileobj=output) as of:
                of.write(body)
            body = output.getvalue()
            output_length = len(body)
            logger.debug("Gzipped Body is now %d bytes "% (output_length, ))

        output_hdfs_file = output_hdfs_working_path + '/' + output_name

        if verbose:
            logger.info("Writing %s (%d bytes)" % (output_name, output_length))


        (tmp_fd, tmp_filename) = tempfile.mkstemp()
        try:
            with open(tmp_filename, "wb") as fp:
                fp.write(body)
        except Exception, e:
            logger.error("FAILED to write temp filename %s with body - %s" % (tmp_filename, str(e)))

        output_start_time = time()
        hadoop_move_from_local(tmp_filename, output_hdfs_file)
        output_end_time = time()
        os.close(tmp_fd)

        if verbose:
            logger.info('read speed %s: time %.3fs, %.3f MB/s' % (input_name, input_end_time - input_start_time, float(input_length) / (input_end_time - input_start_time) / 1000000))
            logger.info('write speed %s: time %.3fs, %.3f MB/s' % (output_name, output_end_time - output_start_time, float(output_length) / (output_end_time - output_start_time) / 1000000))

        marker = input_name

        if marker is not None:
            if verbose:
                logger.info("Writing new marker %s" % (marker, ))
            try:
                with open(marker_filename, "wb") as fp:
                    fp.write(marker + "\n")
            except Exception, e:
                logger.error("FAILED to write marker filename %s with value %s - %s" % (marker_filename, marker, str(e)))

        files_count += 1
        if max_files is not None and files_count >= max_files:
            pending_count = len(input_names) - files_count
            logger.info("Copied max %d files (%d remaining) - ending" % (files_count, pending_count))
            break

    # Finished with files
    if files_count > 0:
        logger.info("Moving finished HDFS directory from %s to %s" % (output_hdfs_working_path, output_hdfs_final_path))
        hadoop_move_dir(output_hdfs_working_path, output_hdfs_final_path)
    else:
        hadoop_rm_dir(output_hdfs_working_path)


def main():
    """Main method"""

    parser = argparse.ArgumentParser(description='Copy metrics from cloud files into HDFS.')
    parser.add_argument('-d', '--debug',
                        action = 'store_true',
                        default = False,
                        help = 'debug messages (default: False)')
    parser.add_argument('-n', '--dryrun',
                        action = 'store_true',
                        default = False,
                        help = 'dryrun (default: False)')
    parser.add_argument('-v', '--verbose',
                        action = 'store_true',
                        default = False,
                        help = 'verbose messages (default: False)')
    parser.add_argument('--min',
                        type = int,
                        default = 10,
                        help = 'min files to copy')
    parser.add_argument('--max',
                        type = int,
                        default = 100,
                        help = 'max files to copy')
    parser.add_argument('-m', '--marker',
                        default = '/tmp/cloud-files-metrics-copy-marker',
                        help = 'marker filename')
    parser.add_argument('-o', '--hdfs-metrics-dir',
                        help = 'HDFS metrics dir')
    parser.add_argument('-z', '--gzip',
                        default = True,
                        help = 'gzip files (default: True)')
    parser.add_argument('config', metavar='CONFIG-FILE', nargs='*',
                        help='config file')
    parser.add_argument('-p', '--pid-file',
                        help = 'PID file',
                        default='/data/pipeline/run/cloud-files-metrics-copy.pid')

    args = parser.parse_args()

    debug = args.debug
    dryrun = args.dryrun
    verbose = args.verbose

    min_files = args.min
    max_files = args.max
    marker_filename = args.marker
    gzip_data = args.gzip
    hdfs_metrics_dir = args.hdfs_metrics_dir
    pid_file = args.pid_file
    config_files = args.config
    if len(config_files) != 1:
        sys.exit('Need cloud files SOURCE configuration file')
    if hdfs_metrics_dir is None:
        sys.exit('Need HDFS metrics dir: -o / --hdfs-metrics-dir DIR')

    ######################################################################

    if debug:
        level=logging.DEBUG
    else:
        level=logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s', level=level)

    try:
        input_config = read_config(config_files[0])
    except BadConfigurationException, e:
        sys.exit("Bad configuration: %s" % (str(e), ))

    rc = pid_file_start(pid_file, dryrun)
    if rc == 0:
        try:
            copy_metrics(input_config, hdfs_metrics_dir, marker_filename,
                         min_files = min_files,
                         max_files = max_files,
                         gzip_data = gzip_data,
                         verbose = verbose,
                         dryrun = dryrun)
        except Exception, e:
            logger.error("copy_metrics failed with exception %s", str(e))
            rc = 1
        finally:
            pid_file_finish(pid_file, dryrun)

    sys.exit(rc)

if __name__ == '__main__':
    main()
