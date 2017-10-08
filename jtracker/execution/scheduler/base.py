from abc import ABCMeta, abstractproperty


class Scheduler(object):
    __metaclass__ = ABCMeta

    def __init__(self, queue_id=None, executor_id=None):
        self._queue_id = queue_id
        self._executor_id = executor_id

    @property
    def executor_id(self):
        return self._executor_id

    @property
    def queue_id(self):
        return self._queue_id

    def next_task(self, job_state=None):
        pass

    def has_next_task(self):
        pass

    def running_jobs(self):
        pass

    def task_completed(self, job_id, task_name, output):
        pass

    def task_failed(self, job_id, task_name, output):
        pass
