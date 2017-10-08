import requests
import json
from jtracker.exceptions import JessNotAvailable
from .base import Scheduler


class JessScheduler(Scheduler):
    """
    Scheduler backed by JTracker Job Execution and Scheduling Services
    """

    def __init__(self, jess_server=None, jt_account=None, queue_id=None, executor_id: str = None):
        super().__init__(queue_id=queue_id, executor_id=executor_id)
        self._jess_server = jess_server
        self._jt_account = jt_account

    @property
    def jess_server(self):
        return self._jess_server

    @property
    def jt_account(self):
        return self._jt_account

    def get_workflow_name(self):
        request_url = "%s/queues/owner/%s/queue/%s" % (self.jess_server.strip('/'),
                                                       self.jt_account, self.queue_id)

        try:
            r = requests.get(url=request_url)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        try:
            queue = json.loads(r.text)
            workflow_name = "%s.%s:%s" % (queue.get('workflow_owner.name'),
                                          queue.get('workflow.name'),
                                          queue.get('workflow.ver'))
            return workflow_name
        except:
            return

    def running_jobs(self, state='running'):
        # call JESS endpoint: /jobs/owner/{owner_name}/queue/{queue_id}/executor/{executor_id}
        request_url = "%s/jobs/owner/%s/queue/%s/executor/%s" % (self.jess_server.strip('/'),
                                                                 self.jt_account, self.queue_id, self.executor_id)

        if state:
            request_url += '?state=%s' % state

        try:
            r = requests.get(url=request_url)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if r.status_code != 200:
            raise Exception('Unable to schedule new task')

        try:
            jobs = json.loads(r.text)
        except:
            jobs = []

        return jobs

    def has_next_task(self):
        request_url = "%s/tasks/owner/%s/queue/%s/executor/%s/has_next_task" % (
                                                                self.jess_server.strip('/'),
                                                                self.jt_account,
                                                                self.queue_id,
                                                                self.executor_id
                                                                )

        try:
            r = requests.get(url=request_url)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if 'true' in r.text.lower():
            return True
        elif 'false' in r.text.lower():
            return False
        else:
            return False

    def next_task(self, job_id=None, job_state=None):
        # job_id is ignored for now

        # GET /tasks/owner/{owner_name}/queue/{queue_id}/next_task
        request_url = "%s/tasks/owner/%s/queue/%s/executor/%s/next_task" % (
                                                                self.jess_server.strip('/'),
                                                                self.jt_account,
                                                                self.queue_id,
                                                                self.executor_id
                                                                )

        if job_state:
            request_url += '?job_state=%s' % job_state

        try:
            r = requests.get(url=request_url)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if r.status_code != 200:
            raise Exception('Unable to schedule new task')

        rv = r.text if r.text else '{}'
        return json.loads(rv)

    def _task_ended(self, job_id, task_name, output=None, success=True):
        if output is None:
            output = dict()

        if success:
            operation = 'task_completed'
        else:
            operation = 'task_failed'

        # PUT /tasks/owner/{owner_name}/queue/{queue_id}/executor/{executor_id}/job/{job_id}/task/{task_name}/task_completed
        request_url = "%s/tasks/owner/%s/queue/%s/executor/%s/job/%s/task/%s/%s" % (
                                                                self.jess_server.strip('/'),
                                                                self.jt_account,
                                                                self.queue_id,
                                                                self.executor_id,
                                                                job_id,
                                                                task_name,
                                                                operation
                                                                )
        try:
            r = requests.put(url=request_url, json=output)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if r.status_code != 200:
            raise Exception('Error occurred: %s' % r.text)

        rv = r.text if r.text else '{}'
        return json.loads(rv)

    def task_completed(self, job_id, task_name, output=None):
        self._task_ended(job_id, task_name, output=output, success=True)

    def task_failed(self, job_id, task_name, output):
        self._task_ended(job_id, task_name, output=output, success=False)
