import uuid

class Worker(object):
    def __init__(self, host_ip=None, host_name=None, cpu_cores=None, memory=None):
        self.host_ip = host_ip
        self.host_name = host_name
        self.cpu_cores = cpu_cores
        self.memory = memory
        self.worker_id = uuid.uuid4()
        self._jtracker = None
        self._current_job = None

    @property
    def jtracker(self):
        return self._jtracker

    @property
    def current_job(self):
        return self._current_job

    def next_job(self, jtracker=None):
        self._jtracker = jtracker

        if self._current_job: return  # if current job exists, return None

        next_job = jtracker.next_job(self, timeout=None)
        if next_job:
            self._current_job = next_job
            return self._current_job
        else:
            return

    def log_job_info(self, status):
        self.jtracker.log_job_info(self.current_job.job_file, info={})  # info must be dict

    def job_json(self):
        return self.jtracker.job_json(self.current_job.job_file)

    def job_failed(self):
        self.jtracker.job_failed(self.current_job.job_file)
        self._current_job = None

    def job_completed(self):
        self.jtracker.job_completed(self.current_job.job_file)
        self._current_job = None

