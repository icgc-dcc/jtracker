import os
import errno
import subprocess
import json
from time import sleep, time
from uuid import uuid4
from random import random
from .. import __version__ as ver


class Worker(object):
    def __init__(self, jt_home=None, account_id=None, scheduler=None, node_id=None):
        self._id = str(uuid4())
        self._jt_home = jt_home
        self._account_id = account_id
        self._node_id = node_id
        self._scheduler = scheduler
        self._task = None

    @property
    def id(self):
        return self._id

    @property
    def jt_home(self):
        return self._jt_home

    @property
    def account_id(self):
        return self._account_id

    @property
    def executor_id(self):
        return self.scheduler.executor_id

    @property
    def queue_id(self):
        return self.scheduler.queue_id

    @property
    def workflow_id(self):
        return self.scheduler.workflow_id

    @property
    def workflow_version(self):
        return self.scheduler.workflow_version

    @property
    def node_id(self):
        return self._node_id

    @property
    def node_dir(self):
        return os.path.join(self.jt_home, 'account.%s' % self.account_id, 'node')

    @property
    def workflow_dir(self):
        return os.path.join(self.node_dir,
                            'workflow.%s' % self.workflow_id,
                            self.workflow_version)

    @property
    def queue_dir(self):
        return os.path.join(self.workflow_dir, 'queue.%s' % self.queue_id)

    @property
    def executor_dir(self):
        return os.path.join(self.queue_dir, 'executor.%s' % self.executor_id)

    @property
    def job_dir(self):
        return os.path.join(self.executor_dir, 'job.%s' % self.task.get('job.id'))

    @property
    def task_dir(self):
        return os.path.join(self.job_dir, 'task.%s' % self.task.get('name'))

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

        self._init_task_dir()

        time_start = int(time())

        print('Worker starts to work on task: %s in job: %s' % (self.task.get('name'), self.task.get('job.id')))

        cmd = "PATH=%s:$PATH %s" % (os.path.join(self.workflow_dir, 'workflow', 'tools'),
                                    json.loads(self.task.get('task_file')).get('command'))

        arg = "'%s'" % self.task.get('task_file') if self.task else ''

        success = True  # assume task complete
        try:
            #print("task command is: %s %s" % (cmd, arg))
            r = subprocess.check_output("%s %s" % (cmd, arg), shell=True)
        except Exception as e:
            success = False  # task failed

        time_end = int(time())

        # get output.json
        with open(os.path.join(self.task_dir, 'output.json'), 'r') as f:
            output = json.load(f)

        _jt_ = {
            'jtcli_version': ver,
            'worker_id': self.id,
            'executor_id': self.executor_id,
            'workflow_id': self.workflow_id,
            'queue_id': self.queue_id,
            'node_id': self.node_id,
            'task_dir': self.task_dir,
            'wall_time': {
                'start': time_start,
                'end': time_end
            }
        }

        output.update({'_jt_': _jt_})

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

    def _init_task_dir(self):
        try:
            os.makedirs(self.task_dir)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

        os.chdir(self.task_dir)
