

class Job(object):

    def __init__(self, job_tracker=None, job_file=None, status=None):
        """
        Initiate a job object
        """
        self._job_tracker = job_tracker
        self._job_file = job_file
        self._status = status

    @property
    def job_tracker(self):
        return self._job_tracker

    @property
    def job_file(self):
        return self._job_file

    @property
    def status(self):
        return self._status

    def log_job_info(self, status):
        self.job_tracker.log_job_info(self.job_file, info={})  # info must be dict

    def job_json(self):
        return self.job_tracker.job_json(self.job_file)

    def job_failed(self):
        self.job_tracker.job_failed(self.job_file)

    def job_completed(self):
        self.job_tracker.job_completed(self.job_file)
