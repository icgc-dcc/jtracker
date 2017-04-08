

class Task(object):

    def __init__(self, name=None, job=None, worker=None, state=None, jtracker=None):
        self._name = name
        self._job = job
        self._worker = worker
        self._jtracker = jtracker
        self._state = state
        self._task_dict = "task_dict to be implemented"  #TODO: populate this


    @property
    def name(self):
        return self._name


    @property
    def jtracker(self):
        return self._jtracker


    @property
    def job(self):
        return self._job


    @property
    def worker(self):
        return self._worker


    @property
    def state(self):
        return self._state


    @property
    def task_dict(self):
        return self.jtracker.get_task_dict( worker_id=self.worker.worker_id,
                                            task_name=self.name,
                                            job_id=self.job.job_id,
                                            job_state=self.job.state
                                        )


    def log_task_info(self, info={}):  # info must be dict
        self.jtracker.log_task_info(self, info=info)


    def task_failed(self):
        self.jtracker.task_failed(self)


    def task_completed(self):
        #self.jtracker.task_completed(self)
        # to be implemented
        return
