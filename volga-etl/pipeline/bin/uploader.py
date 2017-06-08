# Uploader class

from __future__ import print_function

# Standard python modules
import ConfigParser
import StringIO
from collections import defaultdict
import datetime
import glob
import gzip
import hashlib
import logging
import os
import os.path
import re
import tempfile
import time

# PIP installed modules

# Application modules
from hadoop import *


# Globals
logger = logging.getLogger("uploader")

class UploaderException(Exception):
    """ Exception class for Uploader errors """
    pass


REQUIRED_CONFIG_FIELDS = [
    'dest_dir',
    'dest_mode',
    'dest_overwrite',
    'dest_user',
    'process_md5sum',
    'process_sha256sum',
    'source_delete_days',
    'source_dir',
    'source_glob',
    'source_new_seconds',
    'source_processed_policy',
    'source_rename_suffix',
    'transform_gzip'
]



# Utility functions
def str2bool(v):
    return v.lower() in ("yes", "true")

def remove_silently(filename):
    try:
        os.remove(filename)
    except OSError:
        pass

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

def read_body_ungzip(source_name):
    """ Read file content into memory (ungzipping if necessary) """
    body = None
    in_fp = None
    try:
        if source_name.endswith('.gz'):
            in_fp = gzip.GzipFile(filename=source_name, mode='rb')
        else:
            in_fp = open(source_name)
        body = in_fp.read()
    finally:
        if in_fp is not None:
            in_fp.close()
    return body

class MoveGroup(object):
    """ Uploader group class: a set of files (local) transfering to HDFS """

    def __init__(self, srcs, dest, config):
        """ Create a new MoveGroup from srcs to dest with configuration dict """
        self.srcs = srcs
        self.dest = dest
        self.config = config
        self.name = self.config['name']
        logger.debug("Created MoveGroup {name} dest {d} with {n} sources".format(name=self.config['name'], d=str(self.dest), n=len(self.srcs)))


    def _compute_source_hashes(self, src):
        """ Compute hashes for a single source """
        process_sha256sum = str2bool(self.config.get('process_sha256sum'))
        process_md5sum = str2bool(self.config.get('process_md5sum'))

        # Check computed hashes against metadata (from filename)
        hashes = []
        # Calculate some hashes of ORIGINAL input
        body = src['body']
        src_name = src['name']

        if process_sha256sum:
            hashes.append(dict(name='sha256',
                               actual=hashlib.sha256(body).hexdigest()))
        if process_md5sum:
            hashes.append(dict(name='md5',
                               actual=hashlib.md5(body).hexdigest()))
        hash_values = {}
        for h in hashes:
            hname = h['name']
            actual = h['actual']
            hash_values[hname] = actual

            logger.debug("File {file} {hname} hash {actual}".format(file=src_name, hname=hname, actual=actual))

            expected = src.get(hname, None)
            if expected is not None and expected != actual:
                raise UploaderException("File {file} {hname} hash {actual} does not match hash from filename {expected}".format(file=src_name, hname=hname, actual=actual, expected=expected))
        return hash_values

    def process_sources(self):
        """ Process the source files """
        source_dir = self.config.get('source_dir')
        transform_concat_by = self.config.get('transform_concat_by', 'name')

        # Concatenate bodies and free src memory
        src_names = []
        src_dts = []
        sources_size = 0
        cnt = 0
        (body_fd, body_filename) = tempfile.mkstemp()
        try:
            body_fh = os.fdopen(body_fd, "w")
            for src in self.srcs:
                cnt += 1
                src_name = src.get('name')
                src_abs_name = os.path.join(source_dir, src_name)
                src_size = src.get('size')
                src_dt = datetime.datetime.fromtimestamp(src.get('mtime'))

                try:
                    body = read_body_ungzip(src_abs_name)
                except Exception, e:
                    raise UploaderException("Failed to read local file {file} - {e}".format(file=src_abs_name, e=str(e)))

                content_size = len(body)
                logger.debug("Processing  {c} / {n} {src} - {b} bytes. last modified {dt}".format(c=cnt, src=src_name, n=len(self.srcs), b=src_size, dt=src_dt))
                if not src_name.endswith('.gz') and content_size != src_size:
                    raise UploaderException("Read {b} bytes from {src} but stat said {s}".format(b=content_size, src=src_name, s=src_size))

                src['body'] = body
                src['hash_values'] = self._compute_source_hashes(src)
                src['body'] = None

                body_fh.write(body)
                src_size = content_size
                if len(self.srcs) > 1 and \
                  transform_concat_by is not None and \
                  not body.endswith('\n'):
                    body_fh.write('\n')
                    src_size += 1
                body = None

                # Update aggregates
                src_names.append(src_name)
                src_dts.append(src_dt)
                sources_size += src_size

                logger.debug("{c} / {n} Current body size {b} bytes.".format(c=cnt, n=len(self.srcs), b=src_size))
        except UploaderException, e:
            logger.error(str(e))
            remove_silently(body_filename)
            raise
        self.body_filename = body_filename
        self.sources_size = sources_size
        self.src_names = src_names
        self.src_dts = src_dts


    def finish_processing(self, dryrun):
        """ Mark source files processed """
        source_processed_policy = self.config.get('source_processed_policy')
        source_rename_suffix = self.config.get('source_rename_suffix')

        renamed_count = 0
        removed_count = 0
        for src in self.srcs:
            src_name = src.get('name')
            # Do something with the source file
            if source_processed_policy == 'rename':
                source_rename_file = src_name + source_rename_suffix
                if dryrun:
                    logger.debug("Would rename source file {src} to {renamed}".format(src=src_name, renamed=source_rename_file))
                else:
                    logger.debug("Renaming source file {src} to {renamed}".format(src=src_name, renamed=source_rename_file))
                    remove_silently(source_rename_file)
                    os.rename(src_name, source_rename_file)
                renamed_count += 1
            else:
                # policy is delete
                if dryrun:
                    logger.info("Would delete source file {src}".format(src=src_name))
                else:
                    os.remove(name)
                removed_count += 1
        if renamed_count > 0:
            if dryrun:
                logger.info("Would rename {0} processed source files".format(renamed_count))
            else:
                logger.info("Renamed {0} processed source files".format(renamed_count))
        if removed_count > 0:
            if dryrun:
                logger.info("Would delete {0} processed source files".format(removed_count))
            else:
                logger.info("Deleted {0} processed source files".format(removed_count))


class HdfsUploader(object):
    """ Uploader class """

    def __init__(self, config_filename):
        """ Load files from local disk to HDFS using given configuration """

        self.config_filename = config_filename

        try:
            self.config = ConfigParser.SafeConfigParser()
            self.config.read(config_filename)
        except Exception, e:
            raise ValueError("Failed to read config file {file}: {e}".format(file=self.config_filename, e=str(e)))

        self.sections = self.config.sections()
        if len(self.sections) == 0:
            raise ValueError("No upload sections found in config file {file}".format(file=self.config_filename))


    def get_section_config(self, section):
        """Get config for a section (and validate the config is good)

        The config file must include several fields such as
        'source_dir', 'dest_dir' and 'dest_user'.
        """
        if section not in self.sections:
            raise UploaderException("No config found for section {section} in config file {file}".format(section=section, file=self.config_filename))

        config = dict(self.config.items(section))

        for name in REQUIRED_CONFIG_FIELDS:
            if name not in config:
                raise UploaderException('Configuration for {section} is missing required field {field}'.format(section=section, field=name))
        return config


    def scan_source_dir(self, source_dir, source_glob, source_match):
        """ Scan the directory and generate the file metadata """
        os.chdir(source_dir)
        names = glob.glob(source_glob)
        logger.info("Found {n} files in {dir} for glob {glob}".format(n=len(names), dir=source_dir, glob=source_glob))

        if source_match is not None:
            source_re = re.compile(source_match)
            matches = [(n, source_re.match(n)) for n in names]
            metadata = [merge_two_dicts(r.groupdict(), dict(name=n)) for (n, r) in matches if r is not None]
            logger.info("Found {n} files in {dir} for regex {re}".format(n=len(metadata), dir=source_dir, re=source_match))
        else:
            metadata = [dict(name=n) for n in names]

        return sorted(metadata, key=lambda m: m['name'])

    def write_to_hdfs(self, section_config,
                      dest_name, input_filename, source_size,
                      dryrun):
        """ Write body to dest_name returing (name, dest size written) """
        transform_gzip = str2bool(section_config.get('transform_gzip'))
        # overwrite or not?
        dest_overwrite = str2bool(section_config.get('dest_overwrite'))
        # hadoop fs -chown user
        dest_user = section_config.get('dest_user')
        # hadoop -fs chmod mode
        dest_mode = section_config.get('dest_mode')
        dest_size = source_size

        # If gzip flag is set, gzip it (will re-gzip)
        if transform_gzip:
            logger.info("Gzipping {b} bytes body".format(b=source_size))
            try:
                if not dest_name.endswith('.gz'):
                    dest_name += ".gz"
                os.system("gzip {f}".format(f=input_filename))
                input_filename += ".gz"
                st = os.stat(input_filename)
                dest_size = st.st_size
            except Exception, e:
                logger.error("Failed to create gzip data - {e}".format(e=str(e)))
                return 1
            logger.info("Gzipped size {b} bytes / {percent:2.2f}%".format(b=dest_size, percent=(100.0 * dest_size / source_size)))

        if dest_overwrite:
            if dryrun:
                logger.info("Would remove any existing HDFS file {hdfs_file}".format(hdfs_file=dest_name))
            else:
                hadoop_rm_file(dest_name, ignore_errors=True)

        if dryrun:
            logger.info("Would write to HDFS {hdfs_file}".format(hdfs_file=dest_name))
        else:
            logger.debug("Writing to HDFS {hdfs_file}".format(hdfs_file=dest_name))
            hadoop_move_from_local(input_filename, dest_name)

        # Set HDFS ownership and mode
        if dryrun:
            logger.info("Would set HDFS {hdfs_file} user to {user} and mode to {mode}".format(hdfs_file=dest_name, user=dest_user, mode=dest_mode))
        else:
            hadoop_chown(dest_name, dest_user)
            hadoop_chmod(dest_name, dest_mode)

        return (dest_name, dest_size)

    def cleanup_processed_files(self, section_config, dryrun):
        """ Remove previously processed files if they are old enough"""

        source_dir = section_config.get('source_dir')
        source_glob = section_config.get('source_glob')
        source_delete_days = int(section_config.get('source_delete_days'))
        source_rename_suffix = section_config.get('source_rename_suffix')

        os.chdir(source_dir)
        processed_names = glob.glob(source_glob + source_rename_suffix )
        removed_count = 0
        for name in processed_names:
            source_name = os.path.join(source_dir, name)
            dtime = datetime.datetime.fromtimestamp(os.path.getmtime(source_name))
            file_days = (datetime.datetime.now() - dtime).days
            if file_days > source_delete_days:
                if dryrun:
                    logger.debug("Would delete old processed file {0}".format(source_name))
                else:
                    logger.debug("Deleting old processed file {0}".format(source_name))
                    os.remove(name)
                removed_count += 1
        if removed_count > 0:
            if dryrun:
                logger.info("Would delete {0} old processed files".format(removed_count))
            else:
                logger.info("Deleted {0} old processed files".format(removed_count))

    def create_groups(self, section_config, metadata):
        """ Create list of MoveGroup groups for uploading / transforming """
        logger.info("Grouping {n} files".format(n=len(metadata)))

        groups = []

        source_dir = section_config.get('source_dir')

        # Default is group by name i.e. 1 group of copying 1 file
        transform_concat_by = section_config.get('transform_concat_by', 'name')

        dest_dir = section_config.get('dest_dir')
        dest_format = section_config.get('dest_format', '{name}')

        config = section_config.copy()

        # key is group key, value is list of meta items
        concat_groups = defaultdict(list)
        concat_by_fields = transform_concat_by.split(',')
        for meta in metadata:
            group_key = '-'.join([meta.get(k, '') for k in concat_by_fields])
            concat_groups[group_key].append(meta)

        for group_name in sorted(concat_groups.keys()):
            srcs = concat_groups[group_name]
            dest = srcs[0].copy()
            dest_name = dest_format.format(**dest)
            dest.update(name = dest_name,
                        abs_name = os.path.join(dest_dir, dest_name))

            c = config.copy()
            c['name'] = group_name
            m = MoveGroup(srcs, dest, c)
            groups.append(m)

        return groups

    def add_stats(self, source_dir, metadata):
        for meta in metadata:
            name = meta.get('name')

            abs_name = os.path.join(source_dir, name)
            st = os.stat(abs_name)
            source_mtime = st.st_mtime
            meta.update(abs_name = abs_name,
                        size = st.st_size,
                        mtime = source_mtime,
                        dt = datetime.datetime.fromtimestamp(source_mtime))
        return metadata

    def filter_too_new(self, source_new_seconds, max_dt, metadata):
        """ Remove source files that are too new """
        filtered_metadata = []

        now_time = time.time()
        too_new_files = []
        for meta in metadata:
            file_name = meta.get('name')
            # do not process file if it is less than N seconds olda
            file_seconds = now_time - meta.get('mtime')
            file_dt = meta.get('dt').date()
            if file_seconds < source_new_seconds:
                logger.debug("File {name} is too new to process. date {dt} is {file_seconds} seconds old <  {limit_seconds}".format(name=file_name, dt=file_dt, file_seconds=file_seconds, limit_seconds=source_new_seconds))
                too_new_files.append(file_name)
            elif max_dt is not None and file_dt > max_dt:
                logger.debug("File {name} is too new to process. date {dt} is after max date {max_dt}".format(name=file_name, dt=file_dt, max_dt=max_dt))
                too_new_files.append(file_name)
            else:
                filtered_metadata.append(meta)
        if len(too_new_files) > 0:
            logger.info("Skipping {n} files that are too new to process (newer than {limit_seconds} seconds old or after max date {max_dt}".format(n=len(too_new_files), limit_seconds=source_new_seconds, max_dt=max_dt))
        return filtered_metadata

    def validate_hdfs_hashes(self, dest_name, body_size, hash_values):
        """ Check HDFS dest content matches source hashes """
        for (hname, expected) in hash_values.iteritems():
            verify_hash, verify_content_size = hadoop_hash_file(dest_name, hname)
            logger.debug("Got HDFS file {hdfs_file} verification hash {vh} size {vb}".format(hdfs_file=dest_name, vh=verify_hash, vb=verify_content_size))
            if verify_hash != expected:
                raise UploaderException("HDFS file {hdfs_file} {hname} hash {vh} does not match original hash {oh}".format(hdfs_file=dest_name, hname=hname, vh=verify_hash, oh=expected))
            if verify_content_size != body_size:
                raise UploaderException("HDFS file {hdfs_file} {vb} bytes does not match original bytes {ob}".format(hdfs_file=dest_name, vb=verify_content_size, ob=body_size))


    def upload_files(self, section, max_files, max_groups, max_dt,
                     verbose, dryrun):
        """ Upload files for 'section' with given config """

        logger.info('Processing section {section}'.format(section=section))

        section_config = self.get_section_config(section)

        source_dir = section_config.get('source_dir')
        source_glob = section_config.get('source_glob')
        source_match = section_config.get('source_match', None)

        # rename or delete
        source_processed_policy = section_config.get('source_processed_policy')
        source_new_seconds = int(section_config.get('source_new_seconds'))

        transform_gzip = str2bool(section_config.get('transform_gzip'))
        transform_concat_by = section_config.get('transform_concat_by', None)

        # Method max_files overrides config max_files
        if max_files is None:
            max_files = int(section_config.get('max_files', '10'))
        # Method max_groups overrides config max_groups
        if max_groups is None:
            max_groups = int(section_config.get('max_groups', None))

        dest_dir = section_config.get('dest_dir')
        # Destination file format() string
        dest_format = section_config.get('dest_format', '{name}')

        # Scan source area
        metadata = self.scan_source_dir(source_dir, source_glob, source_match)
        if len(metadata) == 0:
            if verbose or dryrun:
                logger.info("No files found in {dir} for glob {glob}".format(dir=source_dir, glob=source_glob))
            return

        metadata = self.add_stats(source_dir, metadata)

        metadata = self.filter_too_new(source_new_seconds, max_dt, metadata)
        if len(metadata) == 0:
            if verbose or dryrun:
                logger.info("No file left in {dir} after removing too new files".format(dir=source_dir))
            return

        # Create list of move groups from metadata
        groups = self.create_groups(section_config, metadata)

        if max_groups is not None and len(groups) >= max_groups:
            logger.info("Pruning work to max {m} out of {n} groups".format(n=len(groups), m=max_groups))
            groups = groups[:max_groups]

        # Prepare destination area
        if dryrun:
            logger.info("Would create HDFS dir {dir}".format(dir=dest_dir))
        else:
            hadoop_make_dir(dest_dir, ignore_errors=True)

        files_count = 0

        # Process each group
        for group in groups:
            logger.info("Processing group {n}".format(n=group.name))
            group.process_sources()
            logger.info("Group contains {n} files with total {s} bytes".format(n=len(group.src_names), s=group.sources_size))

            # Destination for the content
            dest_name = group.dest.get('abs_name')

            if len(group.src_names) > 1:
                if dryrun:
                    logger.info("Would concatenate {nsrcs} files to HDFS {hdfs_file}".format(nsrcs=len(group.src_names), hdfs_file=dest_name))
                else:
                    logger.info("Concatenating {nsrcs} files to HDFS {hdfs_file}".format(nsrcs=len(group.src_names), hdfs_file=dest_name))
            else:
                if dryrun:
                    logger.info("Would move {src} to HDFS {hdfs_file}".format(src=group.src_names[0], hdfs_file=dest_name))
                else:
                    logger.info("Moved {src} to HDFS {hdfs_file}".format(src=group.src_names[0], hdfs_file=dest_name))

            (dest_name, dest_size) = self.write_to_hdfs(section_config,
                                                        dest_name,
                                                        group.body_filename,
                                                        group.sources_size,
                                                        dryrun)
            hash_values = {}
            try:
                if len(group.src_names) == 1 and not dryrun:
                    src = group.srcs[0]
                    src_size = src['size']
                    hash_values = src['hash_values']
                    self.validate_hdfs_hashes(dest_name, src_size, hash_values)
                group.finish_processing(dryrun)
            except UploaderException, e:
                logger.error(str(e))
                hadoop_rm_file(dest_name, ignore_errors=True)
                raise
            finally:
                remove_silently(group.body_filename)

            log_record = {
                'source' : group.src_names,
                'source_size' : group.sources_size,
                'source_dt_first' : min(group.src_dts).isoformat(),
                'source_dt_last' : max(group.src_dts).isoformat(),
                'dest' : dest_name,
                'dest_size' : dest_size,
                'gzipped' : transform_gzip
                }
            if len(group.src_names) == 1 and not dryrun:
                log_record.update(dict(
                    source_sha256_hash = hash_values.get('sha256', None),
                    source_md5_hash = hash_values.get('md5', None),
                ))
            logger.debug(str(log_record))

            files_count += len(group.srcs)
            if max_files is not None and files_count >= max_files:
                pending_count = len(metadata) - files_count
                logger.info("Copied {n} files > {m} max ({remaining} remaining) - ending".format(n=files_count, m=max_files, remaining=pending_count))
                break

        # end for each file

        self.cleanup_processed_files(section_config, dryrun)


    def upload_section(self, section, max_files, max_groups, max_dt,
                       verbose, dryrun):
        """Upload files for the given config @section (default: all
        sections)"""

        if section is not None:
            self.upload_files(section, max_files, max_groups, max_dt,
                              verbose, dryrun)
        else:
            for section in self.sections:
                self.upload_files(section, max_files, max_groups, max_dt,
                                  verbose, dryrun)
