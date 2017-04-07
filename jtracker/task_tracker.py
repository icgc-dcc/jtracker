from .utils import TASK_STATE
import yaml


class TaskTracker(object):
    def __init__(self, descriptor_dict=None, gitracker=None):
        self._name = descriptor_dict.get('name')
        self._gitracker = gitracker


    def _move_task(self, source=None, target=None, task_file=None, timeout=None):
        self.gitracker.move_task(source=source, target=target, task_file=task_file, timeout=timeout)
        return "abc.json"


    # task execution
    def next_task(self, worker, timeout=None):
        # get next task from git repo
        task_file = self._move_task(source=TASK_STATE.QUEUED, target=TASK_STATE.RUNNING, task_file=None, timeout=timeout)

        # once succeeded return a new task object
        if task_file: return task(jtracker=self, worker=worker, task_file=task_file, TASK_STATE=TASK_STATE.RUNNING)


    def task_json(self):
        pass


    def log_task_info(self, task_file=None, timestamp=None, info={}):
        pass


    def task_failed(self, task_file):
        self._move_task(source=TASK_STATE.RUNNING, target=TASK_STATE.FAILED, task_file=task_file)


    def task_completed(self, task_file):
        self._move_task(source=TASK_STATE.RUNNING, target=TASK_STATE.COMPLETED, task_file=task_file)

