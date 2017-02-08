#!/usr/bin/python
#
# Batch job to load files to HDFS (runs as root to change file ownership)
#
# USAGE: hdfs-uploader.py [OPTIONS]
#
# See hdfs-uploader.py -h for details of the OPTIONS
#

from __future__ import print_function

# Standard python modules
import argparse
import datetime
import logging
import sys

# PIP installed modules

# Application modules
from uploader import HdfsUploader
from utc import UTC


# Globals
logger = logging.getLogger("hdfs-uploader")

# Local
def date_from_dt(dt):
    """ Make a date from a YYYY-MM-DD string """
    # There is no strptime for date
    fields = [int(x) for x in dt.split('-')]
    return datetime.date(*fields)

def datetime_now_utc():
    """ Get current datetime in UTC with a timezone """
    return datetime.datetime.utcnow().replace(tzinfo=UTC)

def date_now_utc():
    """ Get current date in UTC """
    return datetime_now_utc().date()


def main():
    """Main method"""

    parser = argparse.ArgumentParser(description='Load files to HDFS')

    parser.add_argument('-c', '--config',
                        default = '/etc/acumen/hdfs-uploader.conf',
                        help = 'uploader config file')
    parser.add_argument('-d', '--debug',
                        action = 'store_true',
                        default = False,
                        help = 'debug messages (default: False)')
    parser.add_argument('-D', '--max-date',
                        help = 'last date to process (default: yesterday)')
    parser.add_argument('-m', '--max',
                        type = int,
                        default = None,
                        help = 'override max number of files to move')
    parser.add_argument('-g', '--max-groups',
                        type = int,
                        default = None,
                        help = 'override max number of groups to move')
    parser.add_argument('-n', '--dryrun',
                        action = 'store_true',
                        default = False,
                        help = 'pretend but do not run (default: False)')
    parser.add_argument('-s', '--section',
                        help = 'section to process')
    parser.add_argument('-v', '--verbose',
                        action = 'store_true',
                        default = True,
                        help = 'verbose messages (default: True)')

    args = parser.parse_args()

    debug = args.debug
    verbose = args.verbose
    dryrun = args.dryrun
    config_file = args.config
    max_files = args.max
    max_dt = args.max_date
    max_groups = args.max_groups
    section = args.section

    ######################################################################

    # Set up logging to console
    log_level = logging.DEBUG if debug else logging.INFO
    log_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s '
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(log_formatter)

    verbose_log_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s '
        '[in %(pathname)s:%(lineno)d]'
    )
    verbose_handler = logging.StreamHandler(sys.stdout)
    verbose_handler.setFormatter(verbose_log_formatter)

    # Set script logger
    logger.addHandler(handler)
    logger.setLevel(log_level)

    # Verbose handler for module logger
    for lgr in [ logging.getLogger('hadoop'), logging.getLogger('uploader') ]:
        lgr.addHandler(verbose_handler)
        lgr.setLevel(log_level)

    if max_dt is not None:
        max_dt = date_from_dt(max_dt)
    else:
        # yesterday UTC
        max_dt = date_now_utc() - datetime.timedelta(days=1)

    h =  HdfsUploader(config_file)
    rc = 0
    if True: #try:
        logger.info("Starting uploading using config file {file}".format(file=config_file))
        h.upload_section(section, max_files, max_groups, max_dt,
                         verbose, dryrun)
        logger.info("Finished uploading")
    #except Exception, e:
    #    logger.error("Uploading failed with exception {e}".format(e=str(e)))
    #    rc = 1

    sys.exit(rc)


if __name__ == '__main__':
    main()
