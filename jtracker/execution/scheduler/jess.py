from .base import Scheduler
import requests
import json
from jtracker.exceptions import JessNotAvailable


class JessScheduler(Scheduler):
    """
    Scheduler backed by JTracker Job Execution and Scheduling Services
    """

    def __init__(self, jess_server=None, jt_account=None, queue=None, executor_id: str = None):
        super().__init__()
        self._jess_server = jess_server
        self._jt_account = jt_account
        self._queue = queue
        self._executor_id = executor_id

    @property
    def jess_server(self):
        return self._jess_server

    @property
    def jt_account(self):
        return self._jt_account

    @property
    def queue(self):
        return self._queue

    @property
    def executor_id(self):
        return self._executor_id

    def running_jobs(self, in_jobs: str = ()):
        # call JESS endpoint: /jobs/owner/{owner_name}/queue/{queue_id}/executor/{executor_id}
        request_url = "%s/jobs/owner/%s/queue/%s/executor/%s" % (self.jess_server.strip('/'),
                                                                 self.jt_account, self.queue, self.executor_id)

        try:
            r = requests.get(url=request_url)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if r.status_code != 200:
            raise Exception('Unable to schedule new task')

        return json.loads(r.text)

    def running_tasks(self, in_jobs: str = ()):
        pass

    def has_next_task(self, in_jobs: str = ()):
        return True

    def next_task_ready(self, in_jobs: str = ()):
        pass

    def next_task(self, job_state=None):
        # GET /tasks/owner/{owner_name}/queue/{queue_id}/next_task
        request_url = "%s/tasks/owner/%s/queue/%s/executor/%s/next_task?job_state=%s" % (
                                                                self.jess_server.strip('/'),
                                                                self.jt_account,
                                                                self.queue,
                                                                self.executor_id,
                                                                job_state
                                                                )

        try:
            r = requests.get(url=request_url)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if r.status_code != 200:
            raise Exception('Unable to schedule new task')

        return json.loads(r.text)
