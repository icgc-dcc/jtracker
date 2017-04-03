from .workflow import Workflow
from .job_tracker import JobTracker


class Scheduler(object):
    def __init__(self, git_repo_url=None, workflow_name=None, workflow_version=None):
        self._git_repo_url = git_repo_url
        self._workflow_name = workflow_name
        self._workflow_version = workflow_version
        self._workflow_yaml = '.'.join([self._workflow_name, self._workflow_version, 'workflow.yaml'])
        self._workflow = Workflow(self._workflow_yaml)

        self._jobs = {
                        'main': self._workflow.main.to_dict()
                     }
        for jt in self.workflow.jobs():
            self._jobs.update({jt.name: jt.to_dict()})


    @property
    def git_repo_url(self):
        return self._git_repo_url

    @property
    def workflow_name(self):
        return self._workflow_name

    @property
    def workflow_version(self):
        return self._workflow_version

    @property
    def workflow(self):
        return self._workflow

