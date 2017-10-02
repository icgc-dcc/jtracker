from time import sleep
from uuid import uuid4
from random import random


class Worker(object):
    def __init__(self, jt_home=None, scheduler=None, node_id=None):
        self._id = str(uuid4())
        self._node_id = node_id
        self._scheduler = scheduler
        self._executor_id = self.scheduler.executor_id
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

    def next_task(self, job_state=None):
        self._task = self.scheduler.next_task(job_state=job_state)
        return self.task

    def run(self, retry=0):
        # get
        if not self.task:
            raise Exception("Must first get a task before calling 'run'")

        print('Worker starts to work on task: %s in job: %s' % (self.task.get('name'), self.task.get('job.id')))
        sleep(random() * 60)

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

        # need to report success properly
        success = True if random() > 0.01 else False

        job_id = self.task.get('job.id')
        task_name = self.task.get('name')
        if success:
            print('Task completed, task: %s, job: %s' % (task_name, job_id))
            self.scheduler.task_completed(job_id=job_id,
                                          task_name=task_name,
                                          output=output)
            exit(0)
        else:
            print('Task failed, task: %s, job: %s' % (task_name, job_id))
            self.scheduler.task_failed(job_id=job_id,
                                       task_name=task_name,
                                       output=output)
            exit(1)
