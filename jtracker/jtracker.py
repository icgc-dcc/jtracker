import os
import uuid
import socket
from .workflow import Workflow
from .task_tracker import TaskTracker
from .gitracker import GiTracker
from .utils import JOB_STATE


class JTracker(object):
    def __init__(self, git_repo_url=None, workflow_name=None, workflow_version=None, jt_home=None):
        self._init_jt_home(jt_home)
        self._init_host_ip()
        self._init_host_id()

        self._gitracker = GiTracker(git_repo_url, workflow_name=workflow_name, workflow_version=workflow_version)

        # TODO: generate JobTrackers from parse self.gitracker.workflow
        self._job_descriptor = self.gitracker.workflow.workflow_dict.get('descriptor')

        self._task_trackers = {}
        for td in self.gitracker.workflow.workflow_dict.get('tasks'):
            self._task_trackers.update(
                            {td.get('name'): TaskTracker(descriptor_dict=td, gitracker=self.gitracker)}
                        )

        self._build_dag()


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
    def task_trackers(self):
        return self._task_trackers


    @property
    def workflow_dag(self):
        return self._workflow_dag


    def next_task(self, worker=None, timeout=None):
        pass


    def _init_jt_home(self, jt_home):
        if not jt_home: jt_home = os.environ['HOME']
        self._jt_home = os.path.join(jt_home, '.jtracker')

        if not os.path.isdir(self.jt_home):
            os.makedirs(self.jt_home)


    def _init_host_id(self):
        host_id_file = os.path.join(self.jt_home, 'host_id')
        if os.path.isfile(host_id_file):
            with open(host_id_file, 'r') as f:
                self._host_id = f.read().rstrip()
        else:
            with open(host_id_file, 'w') as f:
                self._host_id = '%s.%s.%s' % ('host', self.host_ip, str(uuid.uuid4())[:8])
                f.write('%s\n' % self.host_id)


    def _init_host_ip(self):
        self._host_ip = [l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]


    def _build_dag(self):
        # TODO: use task trackers to build a workflow DAG
        self._workflow_dag = None
