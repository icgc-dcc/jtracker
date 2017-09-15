from .base import Scheduler
import requests
import json
from jtracker.exceptions import JessNotAvailable


class JessScheduler(Scheduler):
    """
    Scheduler backed by JTracker Job Execution and Scheduling Services
    """
    def __init__(self, jess_server=None, jt_account=None, queue=None):
        super().__init__()
        self._jess_server = jess_server
        self._jt_account = jt_account
        self._queue = queue

    @property
    def jess_server(self):
        return self._jess_server

    @property
    def jt_account(self):
        return self._jt_account

    @property
    def queue(self):
        return self._queue

    def running_jobs(self, executor_id: str=None, in_jobs: str=()):
        return 1

    def running_tasks(self, executor_id: str=None, in_jobs: str=()):
        pass

    def has_next_task(self, executor_id: str=None, in_jobs: str=()):
        pass

    def next_task(self, worker_id=None, executor_id=None, in_jobs: str=(), only_new_job=False):
        if worker_id is None or executor_id is None:
            raise Exception('Must specify worker ID and executor ID')

        # PUT /tasks/owner/{owner_name}/queue/{queue_id}/next_task
        request_url = '%s/tasks/owner/%s/queue/queue_id/%s' % (self.jess_server.strip('/'),
                                                               self.jt_account, self.queue)

        try:
            r = requests.put(url=request_url, body=worker.to_dict())
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if r.status_code != 200:
            raise Exception('Unable to schedule new task')

        return json.loads(r.text)
