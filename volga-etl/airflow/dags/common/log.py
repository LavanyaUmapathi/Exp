import datetime
import os.path
import errno


def get_log_path(pathname, prefix):
    log_directory = os.path.join(pathname, prefix)

    log_path = '{path}/{date}.log'.format(
        path=log_directory,
        date=datetime.datetime.today().strftime('%Y-%m-%d'))

    latest_log_path = '{path}/latest.log'.format(path=log_directory)
    try:
        os.symlink(log_path, latest_log_path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(latest_log_path)
            os.symlink(log_path, latest_log_path)

    return log_path
