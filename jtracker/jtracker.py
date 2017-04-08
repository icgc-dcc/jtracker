import os
import uuid
import socket
from .workflow import Workflow
from .gitracker import GiTracker
from .job import Job
from .task import Task


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
    def workflow(self):
        return self._workflow


    def next_task(self, worker=None, timeout=None):
        #return self.gitracker.next_task(worker_id=worker.worker_id, timeout=timeout)
        # to be implemented
        return Task()


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
                self._host_id = '%s.%s.%s' % ('host', self.host_ip, str(uuid.uuid4())[:8])
                f.write('%s\n' % self.host_id)


    def _init_host_ip(self):
        try:
            self._host_ip = [l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]
        except:
            self._host_ip = '127.0.0.1'


