#!/usr/bin/env python
"""

Delete cloud files from one container by size (number of files)
or date

USAGE: rm_cloud_files [OPTIONS] CONFIG

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
from datetime import datetime

# local
from cloudfiles import get_conn, BadConfigurationException, read_config


# logger for this program
logger = logging.getLogger("rm_cloud_files")


def rm_files(config, max_files=1, max_days=None, verbose=False):
    """Delete SWIFT files from account @config

    Order files by date and keep up to @max_files
    Remove any files older than @max_days days
    """
    
    connection = get_conn(config)
    container = config['container']

    logger.info("Deleting from container %s" % (container, ))

    (headers, objs) = connection.get_container(container)
    logger.debug("Container Response Headers: %s" % (str(headers), ))

    logger.info("Found %d files in container" % (len(objs), ))

    # sort list of objects
    objs.sort(key = lambda o: o['last_modified'], reverse=True)

    # remove NEW objects from consideration
    if max_days is not None:
        days = int(max_days)
        oldest_ts = time() - (86400 * max_days)
        oldest_dt = datetime.fromtimestamp(oldest_ts)
        oldest_iso = oldest_dt.strftime("%Y-%m-%dT%H:%M:%S")
        objs = [o for o in objs if o['last_modified'] > oldest_iso]
        logger.debug("Truncated list by %d days to %d files" % (max_days, len(objs)))

    # transform list of objects into list of objects to DELETE
    objs_to_delete = []
    if len(objs) > max_files:
        objs_to_delete = objs[max_files:]
        logger.debug("Truncated list by size %d to %d files" % (max_files, len(objs_to_delete)))

    names = [o['name'] for o in objs_to_delete]
    logger.info("Found %d files to delete" % (len(names), ))
    if verbose and len(names) > 0:
        logger.info("File names range: %s ... %s" % (names[0], names[-1]))

    for obj in objs_to_delete:
        logger.debug("Input object: %s" % (str(obj), ))
        name = obj['name']

        logger.info("Deleting %s" % (name, ))
        connection.delete_object(container, name)

    return 0


def main():
    """Main method"""

    parser = argparse.ArgumentParser(description='delete cloud files.')

    parser.add_argument('-m', '--max',
                        type = int,
                        default = 1000,
                        help = 'max number of files to keep (default 1000)')
    parser.add_argument('-y', '--days',
                        default =  None,
                        help = 'max days to preserve (default: no limit)')
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
    max_days = args.days
    config_files = args.config
    debug = args.debug
    verbose = args.verbose
    if len(config_files) != 1:
        sys.exit('Need CONFIG file')

    ######################################################################

    if debug:
        level=logging.DEBUG
    else:
        level=logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s%(message)s', level=level) 

    try:
        config = read_config(config_files[0])
    except BadConfigurationException, e:
        sys.exit("Bad configuration: %s" % (str(e), ))

    rc = rm_files(config,
                  max_files = max_files,
                  max_days = max_days,
                  verbose = verbose)
    sys.exit(rc)

if __name__ == '__main__':
    main()
