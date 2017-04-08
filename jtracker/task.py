

class Task(object):

    def __init__(self, name=None, job=None, worker=None, state=None, jtracker=None):
        self._name = name
        self._job = job
        self._worker = worker
        self._jtracker = jtracker
        self._state = state
        self._task_dict = "task_dict to be implemented"  #TODO: populate this


    @property
    def jtracker(self):
        return self._jtracker


    @property
    def job(self):
        return self._job


    @property
    def task_dict(self):
        return self._task_dict


    @property
    def status(self):
        return self._status


    def log_task_info(self, info={}):  # info must be dict
        self.jtracker.log_task_info(self, info=info)


    def task_failed(self):
        self.jtracker.task_failed(self)


    def task_completed(self):
        #self.jtracker.task_completed(self)
        # to be implemented
        return
