import requests
import json
from jtracker.exceptions import AMSNotAvailable, AccountNameNotFound


class UserClient(object):
    def __init__(self, jt_home=None, jt_account=None, ams_server=None):
        self._jt_home = jt_home
        self._jt_account = jt_account
        self._ams_server = ams_server

    @property
    def jt_home(self):
        return self._jt_home

    @property
    def jt_account(self):
        return self._jt_account

    @property
    def ams_server(self):
        return self._ams_server

    def create(self, user_name=None):
        print('create user: %s' % user_name)
        print('not implemented yet')
