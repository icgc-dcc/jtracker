from .workflow import Workflow
from .job_tracker import JobTracker
from .gitracker import GiTracker


class Scheduler(object):
    def __init__(self, git_repo_url=None, workflow_name=None, workflow_version=None):
        self._gitracker = GiTracker(git_repo_url, workflow_name=workflow_name, workflow_version=workflow_version)

        # TODO: generate JobTrackers from parse self.gitracker.workflow
        self._job_trackers = {
                        'main': self.gitracker.workflow.workflow_dict.get('main')
                     }

        for jt in self.gitracker.workflow.workflow_dict.get('jobs'):
            self.job_trackers.update({jt.get('name'): jt})

    @property
    def gitracker(self):
        return self._gitracker

    @property
    def job_trackers(self):
        return self._job_trackers

    def next_job(self, timeout=None):
        pass
