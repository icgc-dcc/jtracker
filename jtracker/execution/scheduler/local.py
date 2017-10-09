from .base import Scheduler


class LocalScheduler(Scheduler):
    # TODO: not quite working yet
    def __init__(self, job_file=None, workflow_name=None, executor_id=None):
        super().__init__(mode='local')

        # TODO: need to perform some validation
        self._job_file = job_file
        self._workflow_name = workflow_name
        self._executor_id = executor_id

    @property
    def job_file(self):
        return self._job_file

    @property
    def workflow_name(self):
        return self._workflow_name

    @property
    def executor_id(self):
        return self._executor_id
