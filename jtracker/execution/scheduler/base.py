from abc import ABCMeta, abstractproperty


class Scheduler(object):
    __metaclass__ = ABCMeta

    def __init__(self, mode=None):
        self._mode = mode

    @property
    def mode(self):
        return self._mode

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
