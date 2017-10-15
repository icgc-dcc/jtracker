import requests
import json
from jtracker.exceptions import JessNotAvailable, WRSNotAvailable, AMSNotAvailable, AccountNameNotFound
from .base import Scheduler


class JessScheduler(Scheduler):
    """
    Scheduler backed by JTracker Job Execution and Scheduling Services
    """
    def __init__(self, jess_server=None, wrs_server=None, ams_server=None, jt_account=None,
                 queue_id=None, job_id=None, executor_id: str = None):

        super().__init__(mode='sever')

        self._jess_server = jess_server
        self._wrs_server = wrs_server
        self._ams_server = ams_server
        self._jt_account = jt_account
        self._account_id = self._get_owner_id_by_name(jt_account)
        self._queue_id = queue_id
        self._job_id = job_id
        self._executor_id = executor_id
        self._get_workflow_info()
        self._register_executor()

    @property
    def jess_server(self):
        return self._jess_server

    @property
    def wrs_server(self):
        return self._wrs_server

    @property
    def ams_server(self):
        return self._ams_server

    @property
    def jt_account(self):
        return self._jt_account

    @property
    def account_id(self):
        return self._account_id

    @property
    def queue_id(self):
        return self._queue_id

    @property
    def job_id(self):
        return self._job_id

    @property
    def executor_id(self):
        return self._executor_id

    @property
    def workflow_name(self):
        return self._workflow_name

    @property
    def workflow_id(self):
        return self._workflow_id

    @property
    def workflow_version(self):
        return self._workflow_version

    def _get_workflow_info(self):
        request_url = "%s/queues/owner/%s/queue/%s" % (self.jess_server.strip('/'),
                                                       self.jt_account, self.queue_id)

        try:
            r = requests.get(url=request_url)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if r.status_code >= 400:  # need a special response for failed job
            raise Exception('Unable to retrieve Job Queue')

        try:
            queue = json.loads(r.text)
        except:
            raise Exception('Specified Job Queue does not exist')

        if not isinstance(queue, dict):
            raise Exception('Specified Job Queue does not exist')

        self._workflow_id = queue.get('workflow.id')
        self._workflow_version = queue.get('workflow.ver')
        self._workflow_name = "%s.%s:%s" % (queue.get('workflow_owner.name'),
                                            queue.get('workflow.name'),
                                            queue.get('workflow.ver'))

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

        if r.status_code != 200:  # need a special response for failed job
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

    def get_workflow(self):
        request_url = "%s/workflows/id/%s/ver/%s" % (self.wrs_server.strip('/'),
                                                     self.workflow_id, self.workflow_version)

        try:
            r = requests.get(url=request_url)
        except:
            raise WRSNotAvailable('JESS service temporarily unavailable')

        try:
            workflow = json.loads(r.text)
        except:
            raise Exception('Unable to retrieve workflow, id: %s, version: %s' % (
                self.workflow_id, self.workflow_version
            ))

        return workflow

    def _register_executor(self):
        # JESS endpoint: /executors/owner/{owner_name}/queue/{queue_id}
        # later we should really get executor dict by self.to_dict, dict version of the executor object
        executor = {
            'id': self.executor_id  # only ID field for now
        }

        request_url = "%s/executors/owner/%s/queue/%s" % (self.jess_server.strip('/'),
                                                          self.jt_account, self.queue_id)

        try:
            r = requests.post(url=request_url, json=executor)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if r.status_code != 200:
            raise Exception('Failed to register the executor, please make sure it has not been registered before')

    def _get_owner_id_by_name(self, owner_name):
        request_url = '%s/accounts/%s' % (self.ams_server.strip('/'), owner_name)
        try:
            r = requests.get(request_url)
        except:
            raise AMSNotAvailable('AMS service temporarily unavailable')

        if r.status_code != 200:
            raise AccountNameNotFound(owner_name)

        return json.loads(r.text).get('id')

    def cancel_job(self, job_id=None):
        # call JESS endpoint: /jobs/owner/{owner_name}/queue/{queue_id}/job/{job_id}/action
        request_body = {
            'action': 'cancel',
            'executor_id': self.executor_id
        }

        request_url = "%s/jobs/owner/%s/queue/%s/job/%s/action" % (self.jess_server.strip('/'),
                                                                   self.jt_account, self.queue_id, job_id)

        try:
            r = requests.put(request_url, json=request_body)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        print('Job: %s cancelled' % job_id)
