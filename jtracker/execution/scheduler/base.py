

class Scheduler(object):
    def __init__(self):
        pass

    def next_task(self, new_job=False, worker=None):
        if worker is None:
            worker = dict()

    def has_next_task(self, in_jobs: str=()):
        pass

    def running_tasks(self, executor_id: str=None, in_jobs: str=()):
        pass

    def running_jobs(self, executor_id: str=None, in_jobs: str=()):
        pass
