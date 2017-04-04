from .utils import Status
import yaml


class JobTracker(object):

    def __init__(self, name=None, gitracker=None):
        """
        Initiate a JTracker object with a config file
        """
        self._name = name
        self._gitracker = gitracker

    def _move_job(self, source=None, target=None, job_file=None, timeout=None):
        self.gitracker.move_job(source=source, target=target, job_file=job_file, timeout=timeout)
        return "abc.json"

    # job execution
    def next_job(self, worker, timeout=None):
        # get next job from git repo
        job_file = self._move_job(source=Status.QUEUED, target=Status.RUNNING, job_file=None, timeout=timeout)

        # once succeeded return a new job object
        if job_file: return Job(jtracker=self, worker=worker, job_file=job_file, status=Status.RUNNING)

    def job_json(self):
        pass

    def log_job_info(self, job_file=None, timestamp=None, info={}):
        pass

    def job_failed(self, job_file):
        self._move_job(source=Status.RUNNING, target=Status.FAILED, job_file=job_file)

    def job_completed(self, job_file):
        self._move_job(source=Status.RUNNING, target=Status.COMPLETED, job_file=job_file)

    # job creation
    def add_job(self):
        pass

    def enqueue_job(self):
        pass
