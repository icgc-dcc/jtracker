import os
import git
import uuid
import shutil
import tempfile
from .workflow import Workflow


class GiTracker(object):
    def __init__(self, git_repo_url=None, workflow_name=None, workflow_version=None):
        self._git_repo_url = git_repo_url
        self._local_git_path = tempfile.mkdtemp()  # TODO: we can allow user to choose where to clone git
        self.git_clone()

        self._workflow_name = workflow_name
        self._workflow_version = workflow_version
        yaml_file_name = '.'.join([self.workflow_name, self.workflow_version, 'workflow.yaml'])

        self._workflow = Workflow(os.path.join(self.local_git_path, yaml_file_name))


    @property
    def git_repo_url(self):
        return self._git_repo_url


    @property
    def workflow(self):
        return self._workflow


    @property
    def workflow_name(self):
        return self._workflow_name


    @property
    def workflow_version(self):
        return self._workflow_version


    @property
    def local_git_path(self):
        return self._local_git_path


    def move_job(self, source=None, target=None, job_file=None, timeout=None):
        pass


    def git_clone(self):
        repo = git.Repo.init(self.local_git_path)
        origin = repo.create_remote('origin', self.git_repo_url)
        origin.fetch()
        origin.pull(origin.refs[0].remote_head)


    def _parse_workflow_yaml(self):
        self._local_git_path
