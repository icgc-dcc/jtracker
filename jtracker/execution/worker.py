from time import sleep
from uuid import uuid4
from random import random


class Worker(object):
    def __init__(self, jt_home=None, executor_id=None, scheduler=None, node_id=None):
        self._id = str(uuid4())
        self._executor_id = executor_id
        self._node_id = node_id
        self._scheduler = scheduler
        self._task = None

    @property
    def id(self):
        return self._id

    @property
    def executor_id(self):
        return self._executor_id

    @property
    def node_id(self):
        return self._node_id

    @property
    def scheduler(self):
        return self._scheduler

    @property
    def task(self):
        return self._task

    def next_task(self, in_jobs: str=(), only_new_job: bool=False):
        self._task = self.scheduler.next_task(worker_id=self.id, executor_id=self.executor_id,
                                              in_jobs=in_jobs, only_new_job=only_new_job)
        return self.task

    def run(self, retry=0):
        # get
        if not self.task:
            raise Exception("Must first get a task before calling 'run'")

        print('Worker starts to work on task: %s' % self.task.get('id'))
        sleep(random() * 30)

        # TODO: worker completes the task then reports back to server
        output = {
            'worker': {
                'id': self.id,
                'executor_id': self.executor_id,
                'node_id': self.node_id
            },
            'run_stats': {
                'start': 233,
                'end': 23232
            },
            'output': {

            }
        }

        self._task = None
        self.scheduler.task_complete(output)
