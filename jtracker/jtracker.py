from .workflow import Workflow
from .task_tracker import TaskTracker
from .gitracker import GiTracker
from .utils import JOB_STATE


class JTracker(object):
    def __init__(self, git_repo_url=None, workflow_name=None, workflow_version=None):
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


    def _build_dag(self):
        # TODO: use task trackers to build a workflow DAG
        self._workflow_dag = None
