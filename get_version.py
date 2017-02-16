#!/usr/bin/python

import os
from subprocess import Popen, PIPE

DEFAULT_VERSION = "0.0.0"

def _split_version_info(version):
    version_info = version.split(".")
    try:
        major = version_info[0]
        minor = version_info[1]
        rev = 0
        if len(version_info) > 2:
            rev = version_info[2]
    except IndexError:
        return 0, 0, 0
    return int(major), int(minor), int(rev)

def _get_tag():
    if not os.path.isdir('.git'):
        raise RuntimeError("Current directory not part of a git repo")

    return Popen(["git", "describe", "--tags"], stdout=PIPE).communicate()[0]

def _get_version():
    """Generate a version number from the current commit."""
    tag = _get_tag()
    tag_fields = tag.split("-")
    version = "-".join(tag_fields[:-2])
    major, minor, rev = _split_version_info(version)
    (cnt, _) = tag_fields[-2:]
    try:
        rev = rev + int(cnt)
    except ValueError:
        pass
    return "%s.%s.%s" % (major, minor, rev)

if __name__ == "__main__":
    VERSION = _get_version()
    os.environ['VERSION_NUMBER'] = VERSION
    print os.environ['VERSION_NUMBER']
