import os
import json
import uuid
import socket
from .workflow import Workflow
from .gitracker import GiTracker
from .job import Job
from .task import Task
from .utils import JOB_STATE


class JTracker(object):
    def __init__(self, git_repo_url=None, workflow_name=None, workflow_version=None, jt_home=os.environ.get('JT_HOME')):
        self._init_jt_home(jt_home)
        self._init_host_ip()
        self._init_host_id()

        self._gitracker = GiTracker(git_repo_url, workflow_name=workflow_name, workflow_version=workflow_version)

        yaml_file_name = '.'.join([workflow_name, workflow_version, 'workflow.yaml'])
        self._workflow = Workflow(os.path.join(self.gitracker.local_git_path, yaml_file_name))

        # TODO: generate JobTrackers from parse self.gitracker.workflow
        self._job_descriptor = self.workflow.workflow_dict.get('descriptor')


    @property
    def jt_home(self):
        return self._jt_home


    @property
    def host_id(self):
        return self._host_id


    @property
    def host_ip(self):
        return self._host_ip


    @property
    def gitracker(self):
        return self._gitracker


    @property
    def gitracker_home(self):
        return self.gitracker.gitracker_home


    @property
    def workflow(self):
        return self._workflow


    # we will need some methods to handle failed tasks/jobs, such as re-enqueue/suspend tasks/jobs,
    # although the system should have built-in support for automatic retry according to user's settings
    def retry_job(self, job_id=None, force=False):
        pass


    def suspend_job(self, job_id=None):
        """
        put job on hold when finishes its current running task if any
        """
        pass


    def retry_task(self, task_name=None, job_id=None, force=False):
        pass


    def next_job(self, worker=None):
        """
        Usually this is called from internal when a worker requests a new task
        but no task is available from the running jobs, we need to then start a
        new job. It is safe to call this directly from the worker as well.
        Once a job is started, its tasks will be queued for workers to pickup
        """
        return self.gitracker.next_job(worker=worker)


    def next_task(self, worker=None, timeout=None):
        # proper implementation
        task = self.gitracker.next_task(worker=worker, jtracker=self, timeout=timeout)
        if task:
            return task
        else: # not task in running jobs, then start a new job
            # we just need to trigger it here, the client will need to ask for new task again
            self.next_job(worker=worker)
            return False  # return False as we didn't get a task even though a new job might have been started


    def task_completed(self, worker=None, timeout=None):
        ret = self.gitracker.task_completed(
                                        task_name = worker.task.name,
                                        worker_id = worker.worker_id,
                                        job_id = worker.task.job.job_id,
                                        timeout = timeout
                                    )

        # after successfully call task_completed, it's possible the whole job is completed,
        # so always call job_completed on gitracker which will ensure job completes properly
        if ret:
            self.gitracker.job_completed(job_id=worker.task.job.job_id)
            return True
        else:
            return False


    def task_failed(self, worker=None, timeout=None):
        return self.gitracker.task_failed(
                                        task_name = worker.task.name,
                                        worker_id = worker.worker_id,
                                        job_id = worker.job.job_id,
                                        timeout = timeout
                                    )


    def get_job_dict(self, job_id=None, state=None):
        # it may be better to deligate this to gitracker
        with open(self._get_job_json_path(job_id=job_id, state=state), 'r') as f:
            return json.load(f)


    def _get_job_json_path(self, job_id=None, state=None):
        # it may be better to deligate this to gitracker
        file_name = '.'.join([job_id, 'json'])

        if state in (JOB_STATE.BACKLOG, JOB_STATE.QUEUED):
            path = os.path.join(self.gitracker_home, state)
        else:
            path = os.path.join(self.gitracker_home, state, job_id)

        return os.path.join(path, file_name)


    def get_task_dict(self, worker_id=None, task_name=None, job_id=None, job_state=None):
        # it may be better to deligate this to gitracker
        file_path = self._get_task_json_path(worker_id=worker_id, task_name=task_name, job_id=job_id, job_state=job_state)

        with open(file_path, 'r') as f:
            return json.load(f)


    def _get_task_json_path(self, worker_id=None, task_name=None, job_id=None, job_state=None):
        # it may be better to deligate this to gitracker
        if job_state in (JOB_STATE.BACKLOG, JOB_STATE.QUEUED):
            path = os.path.join(self.gitracker_home, job_state)
        else:
            path = os.path.join(self.gitracker_home, job_state, job_id)

        file_name = '.'.join([task_name, 'json'])

        return self._find_file(file_name, path)


    def _init_jt_home(self, jt_home=None):
        if not jt_home: jt_home = os.path.join(os.environ['HOME'], 'jtracker')
        self._jt_home = jt_home

        if not os.path.isdir(self.jt_home):
            os.makedirs(self.jt_home)


    def _init_host_id(self):
        host_id_file = os.path.join(self.jt_home, 'host_id')
        if os.path.isfile(host_id_file):
            with open(host_id_file, 'r') as f:
                self._host_id = f.read().rstrip()
        else:
            with open(host_id_file, 'w') as f:
                self._host_id = str(uuid.uuid4())[:8]
                f.write('%s\n' % self.host_id)


    def _init_host_ip(self):
        try:
            self._host_ip = [l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]
        except:
            self._host_ip = '127.0.0.1'

    @staticmethod
    def _find_file(name, path):
        for root, dirs, files in os.walk(path):
            if name in files:
                return os.path.join(root, name)

