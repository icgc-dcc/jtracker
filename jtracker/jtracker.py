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


    def next_task(self, worker=None, timeout=None):
        #return self.gitracker.next_task(worker_id=worker.worker_id, timeout=timeout)
        # to be implemented
        # psuedo code for now
        task1 = Task(name="task.md5sum_check", job=Job(
                                            job_id = 'job.6b3b673d-612b-4ff6-89d2-038ceec94c5e',
                                            state = JOB_STATE.RUNNING,
                                            jtracker = self
                                        ),
                    worker=worker, jtracker=self)

        # psuedo code for now
        # start task from QUEUED jobs
        """
        task2 = Task(name="task.md5sum_check", job=Job(
                                            job_id = 'job.136414c0-aa43-4169-977b-e022e07a6fc9',
                                            state = JOB_STATE.QUEUED,
                                            jtracker = self
                                        ),
                    worker=worker, jtracker=self)
        """

        return task1


    def get_job_dict(self, job_id=None, state=None):
        with open(self._get_job_json_path(job_id=job_id, state=state), 'r') as f:
            return json.load(f)


    def _get_job_json_path(self, job_id=None, state=None):
        file_name = '.'.join([job_id, 'json'])

        if state in (JOB_STATE.BACKLOG, JOB_STATE.QUEUED):
            path = os.path.join(self.gitracker_home, state)
        else:
            path = os.path.join(self.gitracker_home, state, job_id)

        return os.path.join(path, file_name)


    def get_task_dict(self, worker_id=None, task_name=None, job_id=None, job_state=None):
        file_path = self._get_task_json_path(worker_id=worker_id, task_name=task_name, job_id=job_id, job_state=job_state)

        with open(file_path, 'r') as f:
            return json.load(f)


    def _get_task_json_path(self, worker_id=None, task_name=None, job_id=None, job_state=None):
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

