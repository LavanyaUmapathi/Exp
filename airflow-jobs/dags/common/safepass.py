#!/usr/bin/env python
import rspwsafe
import pyfscache

cache = pyfscache.FSCache('.safepass-cache', days=1)


#cache disabled as cache error handling not implemented
#@cache
def get_safepass(project_id, cred_id):
    """Given PasswordSafe project id and credential description
    returns (host, login, password) tuple of None on error"""

    safe = rspwsafe.PWSafe()
    safe.set_project(project_id)
    json_creds = safe.get_creds()

    results = [item['credential'] for item in json_creds
               if item['credential']['id'] == cred_id]

    try:
        (host, login, passwd) = (results[0]['hostname'],
                                 results[0]['username'],
                                 results[0]['password'])
    except IndexError:
        return None
    return (host, login, passwd)
