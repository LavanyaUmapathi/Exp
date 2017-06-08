#!/usr/bin/env python
"""

Copy cloud files from one (account1, container1) to (account2,
container2) optionally gzipping them on the way and tracking the
files copied using a marker.

USAGE: copy_cloud_files [OPTIONS] SOURCE-CONFIG DEST-CONFIG

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

"""

import argparse
import sys
import logging
from time import time
import StringIO
from gzip import GzipFile

# local
from cloudfiles import get_conn, BadConfigurationException, read_config

# logger for this program
logger = logging.getLogger("copy_cloud_files")


def copy_files(input_config, output_config, marker_filename,
               max_files=1, gzip_data=True, verbose=False):
    """Copy SWIFT files from account / container from @input_config
    to @output_config.

    Copy at most @max_files and store state in file @marker_filename
    If @gzip is True, gzip the files
    """
    
    input_connection = get_conn(input_config)
    input_container = input_config['container']

    output_connection = get_conn(output_config)
    output_container = output_config['container']

    logger.info("Copying from container %s to container %s" % (input_container, output_container))

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
    logger.info("Found %d files to copy" % (len(input_names), ))
    if verbose and len(input_names) > 0:
        logger.info("File names range: %s ... %s" % (input_names[0], input_names[-1]))

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

        output_headers = {}

        if verbose:
            logger.info("Writing %s (%d bytes)" % (output_name, output_length))
        output_start_time = time()
        output_connection.put_object(output_container, output_name, body,
                                     headers = output_headers)
        output_end_time = time()

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
                return 1

        files_count += 1
        if files_count >= max_files:
            pending_count = len(input_names) - files_count
            logger.info("Copied max %d files (%d remaining) - ending" % (files_count, pending_count))
            break

    return 0


def main():
    """Main method"""

    parser = argparse.ArgumentParser(description='Copy cloud files.')

    parser.add_argument('-n', '--max',
                        type = int,
                        default = 1,
                        help = 'max files to copy')
    parser.add_argument('-m', '--marker',
                        default = '/tmp/copy-cloud-files-marker',
                        help = 'marker filename')
    parser.add_argument('-z', '--gzip',
                        default = True,
                        help = 'gzip files (default: True)')
    parser.add_argument('-d', '--debug',
                        action = 'store_true',
                        default = False,
                        help = 'debug messages (default: False)')
    parser.add_argument('-v', '--verbose',
                        action = 'store_true',
                        default = False,
                        help = 'verbose messages (default: False)')
    parser.add_argument('config', metavar='CONFIG-FILE', nargs='+',
                        help='config files')

    args = parser.parse_args()

    max_files = args.max
    marker_filename = args.marker
    gzip_data = args.gzip
    config_files = args.config
    debug = args.debug
    verbose = args.verbose
    if len(config_files) != 2:
        sys.exit('Need SOURCE and DEST config files')

    ######################################################################

    if debug:
        level=logging.DEBUG
    else:
        level=logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s', level=level) 

    try:
        input_config = read_config(config_files[0])
        output_config = read_config(config_files[1])
    except BadConfigurationException, e:
        sys.exit("Bad configuration: %s" % (str(e), ))

    rc = copy_files(input_config, output_config, marker_filename,
                    max_files = max_files,
                    gzip_data = gzip_data,
                    verbose = verbose)
    sys.exit(rc)

if __name__ == '__main__':
    main()
