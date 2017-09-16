

class Scheduler(object):
    def __init__(self):
        pass

    def next_task(self, new_job=False, worker=None, queue=None):
        if worker is None:
            worker = dict()

    def has_next_task(self, in_jobs: str=()):
        pass

    def running_tasks(self, in_jobs: str=()):
        pass

    def running_jobs(self, in_jobs: str=()):
        pass
