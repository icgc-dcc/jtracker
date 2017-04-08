import os
import json
from .utils import JOB_STATE

class Job(object):
    def __init__(self, job_id=None, state=None, jtracker=None):
        self._job_id = job_id
        self._state = state
        self._jtracker = jtracker


    @property
    def job_id(self):
        return self._job_id


    @property
    def state(self):
        return self._state


    @property
    def jtracker(self):
        return self._jtracker


    @property
    def job_dict(self):
        return self.jtracker.get_job_dict(self.job_id, self.state)
