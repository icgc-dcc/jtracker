from time import sleep
from uuid import uuid4
from random import random


class Worker(object):
    def __init__(self, jt_home=None, executor_id=None, scheduler=None):
        self._id = str(uuid4())
        self._executor_id = executor_id
        self._scheduler = scheduler
        self._task = None

    @property
    def id(self):
        return self._id

    @property
    def scheduler(self):
        return self._scheduler

    @property
    def task(self):
        return self._task

    def next_task(self, in_jobs: str=(), only_new_job: bool=False):
        self._task = self.scheduler.next_task(in_jobs=in_jobs, only_new_job=only_new_job)
        return self.task

    def run(self):
        # get
        if not self.task:
            raise Exception("Must first get a task before you can call 'run'")

        print('Worker starts to work on task: %s' % self.task.get('id'))
        sleep(random() * 30)

        # TODO: worker completes the task then reports back to server
        output = {

        }
        return output
