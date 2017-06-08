import os
import json
import getpass
from requests import post, get, put, delete
from ConfigParser import SafeConfigParser
from datetime import datetime


class PWSafeError(Exception):
    """base exception class"""
    def __init__(self, msg):
        super(PWSafeError, self).__init__()
        self.msg = msg

    def __repr__(self):
        return self.msg

    def __str__(self):
        return self.__repr__()


class PWSafeAuth(object):
    """base class that handles authentication to global auth returing token"""

    def __init__(self, addr='https://identity-internal.api.rackspacecloud.com'):
        """
        load credentials and authenticate
        """
        # production
        self.addr = addr
        self.conf = os.path.expanduser('~/.rspwsafe')

        # check for or create config file
        self._check_config()

        # import credentials
        cp = SafeConfigParser()
        cp.read(self.conf)
        self.username = cp.get('auth', 'username').strip()
        self.password = cp.get('auth', 'password').strip()

        # check for cached token
        if cp.has_option('auth', 'token'):
            self.token = cp.get('auth', 'token').strip()
        else:
            self.token = ''
        if cp.has_option('auth', 'expires'):
            self.expires = cp.get('auth', 'expires').strip()
        else:
            self.expires = ''
        # authenticate
        self._login()

    def _check_config(self):
        """
        prompt for credentials if conf file does not exist
        """
        if os.path.exists(self.conf):
            return True

        print("missing configuration file at %s" % self.conf)

        # prompt user for info
        found_user = getpass.getuser()
        u = raw_input("SSO username (%s): " % found_user)
        if u == '':
            u = found_user
        p = getpass.getpass("SSO password: ")

        # write config
        cp = SafeConfigParser()
        cp.add_section('auth')
        cp.set('auth', 'username', u)
        cp.set('auth', 'password', p)

        fp = open(self.conf, 'w')
        cp.write(fp)
        fp.close()

    def _update_token(self):
        """cache authentication token"""
        cp = SafeConfigParser()
        cp.read(self.conf)
        cp.set('auth', 'token', self.token)
        cp.set('auth', 'expires', self.expires)
        fp = open(self.conf, 'w')
        cp.write(fp)
        fp.close()

    def _verify_expire(self):
        """grabs current time and compares to expire time of token"""
        if self.expires == '':
            expire_time = datetime.utcnow()
        else:
            expire_time = datetime.strptime(self.expires, '%Y-%m-%dT%H:%M:%S.%fZ')
        curr_time = datetime.utcnow()
        if curr_time < expire_time:
            return True
        else:
            return False

    def _login(self):
        """
        authenticates if token is invalid also
        """
        payload = {
            "auth": {
                "RAX-AUTH:domain": {
                    "name": "Rackspace"},
                "passwordCredentials": {
                    "username": self.username,
                    "password": self.password
                }
            }
        }
        url = "{0}/v2.0/tokens".format(self.addr)
        headers = {'content-type': 'application/json'}
        if not self._verify_expire():
            r = post(url, data=json.dumps(payload), headers=headers)
            self.auth = r.json()
            self.token = self.auth['access']['token']['id']
            self.expires = self.auth['access']['token']['expires']
            self._update_token()


class PWSafe(object):
    def __init__(self, endpoint="https://passwordsafe.corp.rackspace.com",pid=None):
        self.get_token = PWSafeAuth()
        self.token = self.get_token.token
        self.endpoint = endpoint
        self.headers = {"X-AUTH-TOKEN": self.token,
                        "Content-Type": "application/json",
                        "Accept": "application/json"
                        }
        self.pid = pid

    def set_project(self, pid):
        self.pid = pid

    def create_project(self, name, description):
        url = "%s%s" % (self.endpoint, "/projects.json")
        data = {"project": {"name": name,
                "description": description}}
        data = json.dumps(data)
        r = post(url, data=data, headers=self.headers, verify=False)
        return r

    def get_projects(self):
        projects = []
        page = 1
        while True:
          url = "%s%s%s" % (self.endpoint, "/projects.json?page=", page)
          r = get(url, headers=self.headers, verify=False).json()
          if not r:
              break
          projects.extend(r)
          page+=1
        return projects

    def update_project(self, name, description):
        if self.pid is None:
            print("no project set!!")
            return
        path = "/projects/%s" % self.pid
        url = "%s%s" % (self.endpoint, path)
        data = {"project": {"name": name,
                "description": description}}
        data = json.dumps(data)
        r = put(url, data=data, headers=self.headers, verify=False)
        return r

    def del_project(self):
        if self.pid is None:
            print("no project set!!")
            return
        path = "/projects/%s" % self.pid
        url = "%s%s" % (self.endpoint, path)
        r = delete(url, headers=self.headers, verify=False)
        self.pid = None
        return r

    def create_cred(self, description, username, password, category='', cred_url='', hostname=''):
        if self.pid is None:
            print("no project set!!")
            return
        path = "/projects/%s/credentials" % self.pid
        url = "%s%s" % (self.endpoint, path)
        data = {"credential": {"username": username,
                "description": description, "password": password, "category": category,
                "url": cred_url, "hostname": hostname}}
        data = json.dumps(data)
        r = post(url, data=data, headers=self.headers, verify=False)
        return r

    def get_creds(self):
        if self.pid is None:
            print("no project set!!")
            return
        creds = []
        page = 1
        while True:
            path = "/projects/{0}/credentials.json?page={1}".format(self.pid,page)
            url = "%s%s" % (self.endpoint, path)
            r = get(url, headers=self.headers, verify=False).json()
            if not r:
                break
            creds.extend(r)
            page+=1
        return creds

    def update_cred(self, cid, description, username, password, category='', cred_url='', hostname=''):
        if self.pid is None:
            print("no project set!!")
            return
        path = "/projects/%s/credentials/%s.json" % (self.pid, cid)
        url = "%s%s" % (self.endpoint, path)
        data = {"credential": {"username": username,
                "description": description, "password": password, "category": category,
                "url": cred_url, "hostname": hostname}}
        data = json.dumps(data)
        r = put(url, data=data, headers=self.headers, verify=False)
        return r

    def del_cred(self, cid):
        if self.pid is None:
            print("no project set!!")
            return
        path = "/projects/%s/credentials/%s.json" % (self.pid, cid)
        url = "%s%s" % (self.endpoint, path)
        r = delete(url, headers=self.headers, verify=False)
        return r

    def get_members(self):
        if self.pid is None:
            print("no project set!!")
            return
        path = "/projects/%s/users.json" % self.pid
        url = "%s%s" % (self.endpoint, path)
        r = get(url, headers=self.headers, verify=False).json()
        return r

    def add_members(self, users):
        if self.pid is None:
            print("no project set!!")
            return
        path = "/projects/%s/users/add" % self.pid
        url = "%s%s" % (self.endpoint, path)
        r = post(url, data=users, headers=self.headers, verify=False)
        return r

    def del_member(self, uid):
        if self.pid is None:
            print("no project set!!")
            return
        path = "/projects/%s/users/%s" % (self.pid, uid)
        url = "%s%s" % (self.endpoint, path)
        r = delete(url, headers=self.headers, verify=False)
        return r
