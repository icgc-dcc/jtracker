
class Task(object):

    def __init__(self, jtracker=None, worker=None, job_file=None, status=None):
        """
        Initiate a job object
        """
        self._jtracker = jtracker
        self._worker = worker
        self._job_file = job_file
        self._status = status

    @property
    def jtracker(self):
        return self._jtracker

    @property
    def worker(self):
        return self._worker

    @property
    def job_file(self):
        return self._job_file

    @property
    def status(self):
        return self._status

