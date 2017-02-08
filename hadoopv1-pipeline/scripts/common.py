import os
import subprocess
import logging

# py2.6 does not have this
if "check_output" not in dir( subprocess ):
    def f(*popenargs, **kwargs):
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
    subprocess.check_output = f


HADOOP_COMMAND = 'hadoop'


# logger for this program
logger = logging.getLogger("common")

class HadoopException(Exception):
    """ Exception class for Hadoop errors """
    pass

def pid_file_start(pid_file, dryrun=True):
    """ Check for pid file for this job
    Returns >0 on failure; 0 if ok to proceed; <0 if already running

    HACK - and should check if the contents of the pid file are valid.
    """
    pid = None
    try:
        with open(pid_file) as fp:
            data = fp.read()
            pid = int(data)
    except IOError:
        pid = None
    except Exception, e:
        logger.error("Failed to read contents of pid file %s: %s", pid_file, str(e))
        pid = None

    if pid is not None:
        logger.debug("Existing process %d exists in pid file %s - ending", pid, pid_file)
        return -1

    pid = os.getpid()
    if dryrun:
        logger.debug("DRYRUN would write pid %d to %s", pid, pid_file)
    else:
        logger.debug("Writing pid %d to %s", pid, pid_file)
        try:
            with open(pid_file, "w") as fp:
                fp.write(str(pid)+"\n")
        except Exception, e:
            logger.error("Failed to write pid to %s: %s", pid_file, str(e))
            return 1

    return 0


def pid_file_finish(pid_file, dryrun=True):
    if dryrun:
        logger.info("DRYRUN would remove pid file %s", pid_file)
    else:
        logger.debug("Removing pid file %s", pid_file)
        try:
            os.unlink(pid_file)
        except Exception, e:
            logger.debug("Failed to remove pid file %s: %s", pid_file, str(e))


def hadoop_move_from_local(local, remote):
    """ Move a local file @local to HDFS @remote """
    cmd = [ HADOOP_COMMAND, "fs", "-moveFromLocal", local, remote ]
    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS move from local cmd %s failed with Exception %s" % (str(cmd), str(e)))


def hadoop_make_dir(remote, ignore_errors=False):
    """ Make an HDFS directory @remote """
    cmd = [ HADOOP_COMMAND, "fs", "-mkdir", remote ]
    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        if not ignore_errors:
            raise HadoopException("HDFS make dir cmd %s failed with Exception %s" % (str(cmd), str(e)))


def hadoop_rm_dir(remote):
    """ Remove HDFS directory @remote """
    cmd = [ HADOOP_COMMAND, "fs", "-rmr", remote ]
    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS rm dir cmd %s failed with Exception %s" % (str(cmd), str(e)))


def hadoop_move_dir(old, new):
    """ Rename an HDFS dir @old to @new """
    cmd = [ HADOOP_COMMAND, "fs", "-mv", old, new ]
    logger.debug("Running HDFS cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        raise HadoopException("HDFS move dir cmd %s failed with Exception %s" % (str(cmd), str(e)))

def execute(cmd):
    """ Run a hadoop command @cmd """
    logger.debug("Running cmd " + str(cmd))
    try:
        subprocess.check_output(cmd, stdin=None, stderr=None)
    except subprocess.CalledProcessError, e:
        raise HadoopException("Command %s failed with Exception %s" % (str(cmd), str(e)))
