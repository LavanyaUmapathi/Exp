"""hadoop CLI wrapper functions """

from __future__ import print_function

import gzip
import hashlib
import os
import subprocess
import logging
import StringIO

# py2.6 does not have this
if "check_output" not in dir(subprocess):
    def sbco(*popenargs, **kwargs):
        process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            error = subprocess.CalledProcessError(retcode, cmd)
            error.output = output
            raise error
        return output
    subprocess.check_output = sbco

try:
    from subprocess import DEVNULL # py3k
except ImportError:
    DEVNULL = open(os.devnull, 'wb')


HADOOP_COMMAND = 'hadoop'


# logger for this program
logger = logging.getLogger("hadoop")

class HadoopException(Exception):
    """ Exception class for Hadoop errors """
    pass

def hadoop_copy_from_local(local, remote):
    """ Copy a local file @local to HDFS @remote """
    cmd = [HADOOP_COMMAND, "fs", "-copyFromLocal", local, remote]
    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS copy from local cmd %s failed"
                              " with Exception %s" % (str(cmd), str(e)))


def hadoop_move_from_local(local, remote):
    """ Move a local file @local to HDFS @remote """
    cmd = [HADOOP_COMMAND, "fs", "-moveFromLocal", local, remote]
    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS move from local cmd %s failed"
                              " with Exception %s" % (str(cmd), str(e)))


def hadoop_make_dir(remote, ignore_errors=False):
    """ Make an HDFS directory @remote """
    cmd = [HADOOP_COMMAND, "fs", "-mkdir", remote]

    kwargs = dict(stdin=None, stdout=None)
    if ignore_errors:
        kwargs['stderr'] = DEVNULL

    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.call(cmd, **kwargs)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS make dir cmd %s failed with Exception %s" % (str(cmd), str(e)))


def hadoop_rm_dir(remote, ignore_errors=False):
    """ Remove HDFS directory @remote """
    cmd = [HADOOP_COMMAND, "fs", "-rmr", remote]

    kwargs = dict(stdin=None, stdout=None)
    if ignore_errors:
        kwargs['stderr'] = DEVNULL

    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.call(cmd, **kwargs)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS rm dir cmd %s failed with Exception %s" % (str(cmd), str(e)))


def hadoop_move_dir(old, new):
    """ Rename an HDFS dir @old to @new """
    cmd = [HADOOP_COMMAND, "fs", "-mv", old, new]
    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS move dir cmd %s failed with Exception %s" % (str(cmd), str(e)))


def hadoop_chown(path, user, group=None):
    """ Change ownership of an HDFS file @path to @user """
    if group is None:
        group = user
    cmd = [HADOOP_COMMAND, "fs", "-chown", user+":"+group, path]
    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS chown cmd %s failed with Exception %s" % (str(cmd), str(e)))


def hadoop_chmod(path, mode):
    """ Change ownership of an HDFS file @path to @mode """
    cmd = [HADOOP_COMMAND, "fs", "-chmod", mode, path]
    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS chmod cmd %s failed with Exception %s" % (str(cmd), str(e)))


def hadoop_rm_file(remote, ignore_errors=False, skip_trash=False):
    """ Remove HDFS file @remote """
    cmd = [HADOOP_COMMAND, "fs", "-rm"]
    if skip_trash:
        cmd.append('-skipTrash')
    cmd.append(remote)

    kwargs = dict(stdin=None, stdout=None)
    if ignore_errors:
        kwargs['stderr'] = DEVNULL

    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.call(cmd, **kwargs)
    except subprocess.CalledProcessError, e:
        if not ignore_errors:
            raise HadoopException("HDFS rm cmd %s failed with Exception %s" % (str(cmd), str(e)))


def hadoop_hash_file(remote, digest):
    """ Get a HASH @digest for an HDFS file @remote (gunzipping if necessary)
    returning tuple of (sha256 hash in hex string form, size in bytes) """

    gunzip_remote = remote.endswith(".gz")
    cmd = [HADOOP_COMMAND, "fs", "-cat", remote]
    logger.debug("Running HDFS cmd " + str(cmd))
    result = None
    try:
        process = subprocess.Popen(cmd, stdin=DEVNULL, stderr=None,
                                   stdout=subprocess.PIPE)
        content, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            error = subprocess.CalledProcessError(retcode, cmd)
            error.content = content
            raise error

        if gunzip_remote:
            string_f = StringIO.StringIO(content)
            gz = gzip.GzipFile(fileobj=string_f)
            content = gz.read()
            gz.close()

        size = len(content)
        h = hashlib.new(digest)
        h.update(content)
        content = None

        result = (h.hexdigest(), size)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS cat cmd %s failed with Exception %s" % (str(cmd), str(e)))

    return result
