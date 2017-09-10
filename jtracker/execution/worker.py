from time import sleep
from uuid import uuid4
from random import random


class Worker(object):
    def __init__(self, jt_home=None, task=None, executor_id=None, queue_id=None, jess_server=None):
        self._id = str(uuid4())
        self._task = task
        self._executor_id = executor_id

    @property
    def id(self):
        return self._id

    def run(self):
        # get inputs from server

        print('Worker is working ...')
        sleep(random() * 30)

        # TODO: worker completes the task then reports back to server
        output = {

        }
        return output
