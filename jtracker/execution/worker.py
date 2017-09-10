from time import sleep
from uuid import uuid4


class Worker(object):
    def __init__(self, jt_home=None, task=None, executor_id=None, queue_id=None, jess_server=None):
        self._id = str(uuid4())
        self._task = task
        self._executor_id = executor_id

    @property
    def id(self):
        return self._id

    def run(self):
        print('Worker is working ...')
        sleep(3)
        return
