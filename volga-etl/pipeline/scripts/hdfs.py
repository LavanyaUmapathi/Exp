#!/usr/bin/env python

import common


HDFS_COMMAND = 'hdfs'

def _exec(params):
    """ Run an HDFS command with @params """
    cmd = [ HDFS_COMMAND, 'dfs']
    cmd.extend(params)
    common.execute(cmd)

def file_exists(remote):
    """ Return true if file @remote exists """
    cmd = [ "-test", "-e", remote ]
    try:
        _exec(cmd)
        return True
    except:
        return False

def rm_file(remote):
    """ Remove HDFS file @remote """
    cmd = [ "-rm", remote ]
    _exec(cmd)

def move_from_local(local, remote):
    """ Move a local file @local to HDFS @remote """
    cmd = [ "-moveFromLocal", local, remote ]
    _exec(cmd)

def make_dir(remote, ignore_errors=False):
    """ Make an HDFS directory @remote """
    cmd = [ "-mkdir", remote ]
    _exec(cmd)
