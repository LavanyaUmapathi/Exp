""" Cloud files utility functions """

import json

# local
from swiftclient import Connection


def get_conn(options):
    """
    Return a SWIFT connection from the options dict.
    """
    return Connection(options['authurl'],
                      options['user'],
                      options['key'],
                      options['retries'],
                      auth_version = options['auth_version'],
                      os_options = options['os_options'],
                      snet = options['snet'],
                      cacert = options['os_cacert'],
                      insecure = options['insecure'],
                      ssl_compression = options['ssl_compression'])

class BadConfigurationException(Exception):
    pass

def read_config(filename):
    """ Read a cloudfiles environment from a file name returning a dict
    suitable for switclient.client Connection()
    """

    REQUIRED_FIELDS = ['authurl', 'user', 'key']

    config = {
        # No defaults for these: required
        #'authurl' : '',
        #'user' : '',
        #'key' : '',
        'retries' : 5,
        'auth_version' : '1',
        'os_options' : None,
        'snet' : False,
        'os_cacert' : None,
        'insecure' : None,
        'ssl_compression' : True
      }

    try:
        with open(filename) as fp:
            data = json.load(fp)
        config.update(data)
    except Exception, e:
        raise BadConfigurationException("Failed to read config file %s - %s" % (filename, str(e)))

    ok = True
    for k in REQUIRED_FIELDS:
        if k not in config:
            raise BadConfigurationException("Missing config key %s" % (k, ))
            ok = False

    if not ok:
        raise BadConfigurationException()

    return config
