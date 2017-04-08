import os
import json
from .utils import JOB_STATE

class Job(object):
    def __init__(self, job_id=None, state=None, jtracker=None):
        self._job_id = job_id
        self._state = state
        self._jtracker = jtracker
        self._job_dict = self._get_job_dict()


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
        return self._job_dict


    def _get_job_dict(self):
        file_name = '.'.join([self.job_id, 'json'])

        if self.state in (JOB_STATE.BACKLOG, JOB_STATE.QUEUED):
            path = os.path.join(self.jtracker.gitracker.gitracker_home, self.state)
        else:
            path = os.path.join(self.jtracker.gitracker.gitracker_home, self.state, self.job_id)

        with open(os.path.join(path, file_name), 'r') as f:
            return json.load(f)

